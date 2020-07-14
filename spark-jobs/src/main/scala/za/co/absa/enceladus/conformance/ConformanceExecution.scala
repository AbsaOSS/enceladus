/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.conformance

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.common.Constants.{InfoDateColumn, InfoDateColumnString, InfoVersionColumn, ReportDateFormat}
import za.co.absa.enceladus.common.RecordIdGeneration._
import za.co.absa.enceladus.common.config.{JobConfigParser, PathConfig}
import za.co.absa.enceladus.common.plugin.menas.MenasPlugin
import za.co.absa.enceladus.common.{CommonJobExecution, Constants, RecordIdGeneration}
import za.co.absa.enceladus.conformance.config.{ConformanceConfig, ConformanceParser}
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.performance.PerformanceMetricTools
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait ConformanceExecution extends CommonJobExecution {
  private val conformanceReader = new ConformancePropertiesProvider
  private val sourceId = SourcePhase.Conformance

  protected def prepareConformance[T](preparationResult: PreparationResult)
                                     (implicit dao: MenasDAO,
                                      cmd: ConformanceParser[T],
                                      fsUtils: FileSystemVersionUtils,
                                      spark: SparkSession
                                     ): Unit = {
    // Enable Control Framework
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper

    spark.enableControlMeasuresTracking(s"${preparationResult.pathCfg.inputPath}/_INFO")
      .setControlMeasuresWorkflow(sourceId.toString)

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)

    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(
      conf,
      cmd.datasetName,
      cmd.datasetVersion,
      cmd.reportDate,
      preparationResult.reportVersion)
  }

  protected def readConformanceInputData(pathCfg: PathConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(pathCfg.inputPath)
  }

  protected def conform(inputData: DataFrame, preparationResult: PreparationResult)
                       (implicit spark: SparkSession, cmd: ConformanceConfig, dao: MenasDAO): DataFrame = {
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    implicit val featureSwitcher: FeatureSwitches = conformanceReader.readFeatureSwitches()

    Try {
      handleControlInfoValidation()
      DynamicInterpreter.interpret(preparationResult.dataset, inputData)
    } match {
      case Failure(e: ValidationException) =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(sourceId.toString, e.getMessage, e.techDetails)
        throw e
      case Failure(NonFatal(e)) =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(sourceId.toString, e.getMessage, sw.toString)
        throw e
      case Success(conformedDF) =>
        if (SchemaUtils.fieldExists(Constants.EnceladusRecordId, conformedDF.schema)) {
          conformedDF // no new id regeneration
        } else {
          RecordIdGeneration.addRecordIdColumnByStrategy(conformedDF, Constants.EnceladusRecordId, recordIdGenerationStrategy)
        }
    }
  }

  protected def processConformanceResult(args: Array[String],
                                         result: DataFrame,
                                         preparationResult: PreparationResult,
                                         menasCredentials: MenasCredentials)
                                        (implicit spark: SparkSession,
                                         cmd: ConformanceConfig,
                                         fsUtils: FileSystemVersionUtils): Unit = {
    val cmdLineArgs: String = args.mkString(" ")

    PerformanceMetricTools.addJobInfoToAtumMetadata(
      "conform",
      preparationResult.pathCfg.inputPath,
      preparationResult.pathCfg.outputPath,
      menasCredentials.username, cmdLineArgs
    )

    val withPartCols = result
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(cmd.reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(preparationResult.reportVersion))

    val recordCount = result.lastCheckpointRowCount match {
      case None => withPartCols.count
      case Some(p) => p
    }
    if (recordCount == 0) {
      handleEmptyOutput(SourcePhase.Conformance)
    }

    // ensure the whole path but version exists
    fsUtils.createAllButLastSubDir(preparationResult.pathCfg.outputPath)

    withPartCols.write.parquet(preparationResult.pathCfg.outputPath)

    val publishDirSize = fsUtils.getDirectorySize(preparationResult.pathCfg.outputPath)
    preparationResult.performance.finishMeasurement(publishDirSize, recordCount)
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(
      spark,
      "conform",
      preparationResult.pathCfg.inputPath,
      preparationResult.pathCfg.outputPath,
      menasCredentials.username, cmdLineArgs
    )

    withPartCols.writeInfoFile(preparationResult.pathCfg.outputPath)
    writePerformanceMetrics(preparationResult.performance, cmd)

    if (conformanceReader.isAutocleanStdFolderEnabled()) {
      fsUtils.deleteDirectoryRecursively(preparationResult.pathCfg.inputPath)
    }
    log.info(s"$sourceId finished successfully")
  }

  override protected def getPathCfg[T](cmd: JobConfigParser[T], conformance: Dataset, reportVersion: Int): PathConfig = {
    val confCmd = cmd.asInstanceOf[ConformanceParser[T]]
    PathConfig(
      outputPath = buildPublishPath(confCmd, conformance, reportVersion),
      inputPath = getStandardizationPath(cmd, reportVersion)
    )
  }

  def buildPublishPath[T](cmd: ConformanceParser[T],
                          ds: Dataset,
                          reportVersion: Int): String = {
    val infoDateCol: String = InfoDateColumn
    val infoVersionCol: String = InfoVersionColumn

    (cmd.publishPathOverride, cmd.folderPrefix) match {
      case (None, None) =>
        s"${ds.hdfsPublishPath}/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
      case (None, Some(folderPrefix)) =>
        s"${ds.hdfsPublishPath}/$folderPrefix/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
      case (Some(publishPathOverride), _) =>
        publishPathOverride
    }
  }
}
