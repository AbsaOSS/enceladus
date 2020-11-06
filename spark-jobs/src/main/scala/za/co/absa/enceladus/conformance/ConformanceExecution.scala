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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits.DataSetWrapper
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.common.Constants.{InfoDateColumn, InfoDateColumnString, InfoVersionColumn, ReportDateFormat}
import za.co.absa.enceladus.common.RecordIdGeneration._
import za.co.absa.enceladus.common.config.{JobConfigParser, PathConfig}
import za.co.absa.enceladus.common.plugin.menas.MenasPlugin
import za.co.absa.enceladus.common.{CommonJobExecution, Constants, RecordIdGeneration}
import za.co.absa.enceladus.conformance.config.{ConformanceConfig, ConformanceConfigParser}
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization_conformance.config.StandardizationConformanceConfig
import za.co.absa.enceladus.utils.config.PathWithFs
import za.co.absa.enceladus.utils.fs.FileSystemUtils.FileSystemExt
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
                                      cmd: ConformanceConfigParser[T],
                                      spark: SparkSession): Unit = {

    val stdFsUtils = preparationResult.pathCfg.standardization.fileSystem.toFsUtils

    val stdDirSize = stdFsUtils.getDirectorySize(preparationResult.pathCfg.standardization.path)
    preparationResult.performance.startMeasurement(stdDirSize)

    log.info(s"standardization path: ${preparationResult.pathCfg.standardization.path}")
    log.info(s"publish path: ${preparationResult.pathCfg.publish.path}")

    // Enable Control Framework
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper

    // reinitialize Control Framework in case of combined job
    if (cmd.isInstanceOf[StandardizationConformanceConfig]) {
      spark.disableControlMeasuresTracking()
    }

    // InputPath is standardizationPath in the combined job
    spark.enableControlMeasuresTracking(s"${preparationResult.pathCfg.standardization.path}/_INFO")
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

  override def getPathConfig[T](cmd: JobConfigParser[T], dataset: Dataset, reportVersion: Int)
                               (implicit hadoopConf: Configuration): PathConfig = {
    val initialConfig = super.getPathConfig(cmd, dataset, reportVersion)
    cmd.asInstanceOf[ConformanceConfig].publishPathOverride match {
      case None => initialConfig
      case Some(providedPublishPath) => initialConfig.copy(publish = PathWithFs.fromPath(providedPublishPath))
    }
  }

  override def validateOutputPath(pathConfig: PathConfig): Unit = {
    // Conformance output is validated in the publish FS
    validateIfPathAlreadyExists(pathConfig.publish)
  }

  protected def readConformanceInputData(pathCfg: PathConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(pathCfg.standardization.path)
  }

  protected def conform[T](inputData: DataFrame, preparationResult: PreparationResult)
                          (implicit spark: SparkSession, cmd: ConformanceConfigParser[T], dao: MenasDAO): DataFrame = {
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    implicit val featureSwitcher: FeatureSwitches = conformanceReader.readFeatureSwitches()
    implicit val stdFs = preparationResult.pathCfg.standardization.fileSystem

    Try {
      handleControlInfoValidation()
      DynamicInterpreter().interpret(preparationResult.dataset, inputData)
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

  protected def processConformanceResult[T](args: Array[String],
                                            result: DataFrame,
                                            preparationResult: PreparationResult,
                                            menasCredentials: MenasCredentials)
                                           (implicit spark: SparkSession,
                                            cmd: ConformanceConfigParser[T]): Unit = {
    val cmdLineArgs: String = args.mkString(" ")
    val stdFs = preparationResult.pathCfg.standardization.fileSystem
    val publishFs = preparationResult.pathCfg.publish.fileSystem

    PerformanceMetricTools.addJobInfoToAtumMetadata(
      "conform",
      preparationResult.pathCfg.standardization,
      preparationResult.pathCfg.publish.path,
      menasCredentials.username, cmdLineArgs
    )

    val withPartCols = result
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(cmd.reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(preparationResult.reportVersion))

    val recordCount: Long = result.lastCheckpointRowCount match {
      case None => withPartCols.count
      case Some(p) => p
    }
    if (recordCount == 0) {
      handleEmptyOutput(SourcePhase.Conformance)
    }

    withPartCols.write.parquet(preparationResult.pathCfg.publish.path)

    val publishDirSize = publishFs.toFsUtils.getDirectorySize(preparationResult.pathCfg.publish.path)
    preparationResult.performance.finishMeasurement(publishDirSize, recordCount)
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(
      spark,
      "conform",
      preparationResult.pathCfg.standardization,
      preparationResult.pathCfg.publish,
      menasCredentials.username, cmdLineArgs
    )

    withPartCols.writeInfoFile(preparationResult.pathCfg.publish.path)(publishFs)
    writePerformanceMetrics(preparationResult.performance, cmd)

    if (conformanceReader.isAutocleanStdFolderEnabled()) {
      stdFs.toFsUtils.deleteDirectoryRecursively(preparationResult.pathCfg.standardization.path)
    }
    log.info(s"$sourceId finished successfully")
  }
}
