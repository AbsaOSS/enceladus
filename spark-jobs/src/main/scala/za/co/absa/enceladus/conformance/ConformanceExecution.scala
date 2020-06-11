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

import org.apache.spark.sql
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.common.Constants.{InfoDateColumn, InfoDateColumnString, InfoVersionColumn, ReportDateFormat}
import za.co.absa.enceladus.common.RecordIdGeneration.getRecordIdGenerationStrategyFromConfig
import za.co.absa.enceladus.common.{CommonJobExecution, Constants, PathConfig, RecordIdGeneration}
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait ConformanceExecution extends CommonJobExecution {
  protected implicit val conformanceStepName: String = "Conformance"
  private val conformanceReader = new ConformanceReader(log, conf)

  def getPathCfg(cmd: ConfCmdConfigT, conformance: Dataset, reportVersion: Int): PathConfig =
    PathConfig(
      outputPath = buildPublishPath(cmd, conformance, reportVersion),
      inputPath = getStandardizationPath(cmd.jobConfig, reportVersion)
    )

  def buildPublishPath(cmd: ConfCmdConfigT, ds: Dataset, reportVersion: Int): String = {
    val infoDateCol: String = InfoDateColumn
    val infoVersionCol: String = InfoVersionColumn

    (cmd.confConfig.publishPathOverride, cmd.jobConfig.folderPrefix) match {
      case (None, None) =>
        s"${ds.hdfsPublishPath}/$infoDateCol=${cmd.jobConfig.reportDate}/$infoVersionCol=$reportVersion"
      case (None, Some(folderPrefix)) =>
        s"${ds.hdfsPublishPath}/$folderPrefix/$infoDateCol=${cmd.jobConfig.reportDate}/$infoVersionCol=$reportVersion"
      case (Some(publishPathOverride), _) =>
        publishPathOverride
    }
  }

  protected def conform(conformance: Dataset, inputData: sql.Dataset[Row])
                       (implicit spark: SparkSession, cmd: ConfCmdConfigT, dao: MenasDAO): DataFrame = {
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)
    implicit val featureSwitcher: FeatureSwitches = conformanceReader.readFeatureSwitches()

    Try {
      handleControlInfoValidation()
      DynamicInterpreter.interpret(conformance, inputData)
    } match {
      case Failure(e: ValidationException) =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(conformanceStepName, e.getMessage, e.techDetails)
        throw e
      case Failure(NonFatal(e)) =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(conformanceStepName, e.getMessage, sw.toString)
        throw e
      case Success(conformedDF) =>
        if (SchemaUtils.fieldExists(Constants.EnceladusRecordId, conformedDF.schema)) {
          conformedDF // no new id regeneration
        } else {
          RecordIdGeneration.addRecordIdColumnByStrategy(conformedDF, Constants.EnceladusRecordId, recordIdGenerationStrategy)
        }
    }
  }

  protected def processConformanceResult(result: DataFrame,
                                         performance: PerformanceMeasurer,
                                         pathCfg: PathConfig,
                                         reportVersion: Int,
                                         menasCredentials: MenasCredentials)
                                        (implicit spark: SparkSession,
                                         cmd: ConfCmdConfigT,
                                         fsUtils: FileSystemVersionUtils): Unit = {
    val cmdLineArgs: String = cmd.jobConfig.args.mkString(" ")

    PerformanceMetricTools.addJobInfoToAtumMetadata("conform",
      pathCfg.inputPath, pathCfg.outputPath, menasCredentials.username, cmdLineArgs)

    val withPartCols = result
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(cmd.jobConfig.reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(cmd.jobConfig.reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(reportVersion))

    val recordCount = result.lastCheckpointRowCount match {
      case None => withPartCols.count
      case Some(p) => p
    }
    if (recordCount == 0) {
      handleEmptyOutputAfterStep()
    }

    // ensure the whole path but version exists
    fsUtils.createAllButLastSubDir(pathCfg.outputPath)

    withPartCols.write.parquet(pathCfg.outputPath)

    val publishDirSize = fsUtils.getDirectorySize(pathCfg.outputPath)
    performance.finishMeasurement(publishDirSize, recordCount)
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "conform",
      pathCfg.inputPath, pathCfg.outputPath, menasCredentials.username, cmdLineArgs)

    withPartCols.writeInfoFile(pathCfg.outputPath)
    writePerformanceMetrics(performance, cmd.jobConfig)

    if (conformanceReader.isAutocleanStdFolderEnabled()) {
      fsUtils.deleteDirectoryRecursively(pathCfg.inputPath)
    }
  }
}
