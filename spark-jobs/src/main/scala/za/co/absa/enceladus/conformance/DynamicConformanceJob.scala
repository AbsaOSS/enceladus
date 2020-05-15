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
import java.text.MessageFormat

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SPARK_VERSION, sql}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits.{DataSetWrapper, StringToPath}
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.common.Constants._
import za.co.absa.enceladus.common.RecordIdGeneration._
import za.co.absa.enceladus.common.plugin.menas.MenasPlugin
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.common.{Constants, RecordIdGeneration}
import za.co.absa.enceladus.common.ControlInfoValidation
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches, ThreeStateSwitch}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.plugins.builtin.utils.SecureKafka
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.general.ProjectMetadataTools
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object DynamicConformanceJob {
  TimeZoneNormalizer.normalizeJVMTimeZone()

  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val conf: Config = ConfigFactory.load()
  private val menasBaseUrls = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))

  def main(args: Array[String]) {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    SecureKafka.setSecureKafkaProperties(conf)

    SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

    implicit val cmd: ConfCmdConfig = ConfCmdConfig.getCmdLineArguments(args)
    implicit val spark: SparkSession = obtainSparkSession() // initialize spark
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    val enableCF: Boolean = true

    dao.authenticate()

    // get the dataset definition
    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion)
    val dateTokens = cmd.reportDate.split("-")

    val reportVersion = cmd.reportVersion match {
      case Some(version) => version
      case None          => inferVersion(conformance.hdfsPublishPath, cmd.reportDate)
    }

    val pathCfg = PathCfg(
      publishPath = buildPublishPath(InfoDateColumn, InfoVersionColumn, cmd, conformance, reportVersion),
      stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"), cmd.datasetName,
        cmd.datasetVersion.toString, cmd.reportDate, reportVersion.toString)
    )
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    log.info(s"stdpath = ${pathCfg.stdPath}, publishPath = ${pathCfg.publishPath}")
    // die before performing any computation if the output path already exists
    if (fsUtils.hdfsExists(pathCfg.publishPath)) {
      throw new IllegalStateException(
        s"Path ${pathCfg.publishPath} already exists. Increment the run version, or delete ${pathCfg.publishPath}")
    }

    initFunctionalExtensions(reportVersion, pathCfg)
    val performance = initPerformanceMeasurer(pathCfg.stdPath)

    // load data for input and mapping tables
    val inputData = spark.read.parquet(pathCfg.stdPath)

    try {
      val result = conform(conformance, inputData, enableCF, recordIdGenerationStrategy)

      PerformanceMetricTools.addJobInfoToAtumMetadata("conform",
        pathCfg.stdPath, pathCfg.publishPath, menasCredentials.username, args.mkString(" "))

      processResult(result, performance, pathCfg, reportVersion, args.mkString(" "), menasCredentials)

      log.info("Conformance finished successfully")
    } finally {

      MenasPlugin.runNumber.foreach { runNumber =>
        val name = cmd.datasetName
        val version = cmd.datasetVersion
        menasBaseUrls.foreach { menasBaseUrl =>
          log.info(s"Menas API Run URL: $menasBaseUrl/api/runs/$name/$version/$runNumber")
          log.info(s"Menas UI Run URL: $menasBaseUrl/#/runs/$name/$version/$runNumber")
        }
      }
    }
  }

  private def isExperimentalRuleEnabled()(implicit cmd: ConfCmdConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.experimentalMappingRule,
      "conformance.mapping.rule.experimental.implementation",
      defaultValue = false)
    log.info(s"Experimental mapping rule enabled = $enabled")
    enabled
  }

  private def isCatalystWorkaroundEnabled()(implicit cmd: ConfCmdConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.isCatalystWorkaroundEnabled,
      "conformance.catalyst.workaround",
      defaultValue = true)
    log.info(s"Catalyst workaround enabled = $enabled")
    enabled
  }

  private def isAutocleanStdFolderEnabled()(implicit cmd: ConfCmdConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.autocleanStandardizedFolder,
      "conformance.autoclean.standardized.hdfs.folder",
      defaultValue = false)
    log.info(s"Autoclean standardized HDFS folder = $enabled")
    enabled
  }

  private def broadcastingStrategyMode: ThreeStateSwitch = {
    ThreeStateSwitch(conf.getString("conformance.mapping.rule.broadcast"))
  }

  private def broadcastingMaxSizeMb: Int = {
    conf.getInt("conformance.mapping.rule.max.broadcast.size.mb")
  }

  /**
    * Returns an effective value of a parameter according to the following priorities:
    * - Command line arguments [highest]
    * - Configuration file (application.conf)
    * - Global default [lowest]
    *
    * @param cmdParameterOpt An optional value retrieved from command line arguments
    * @param configKey       A key in a configuration file
    * @param defaultValue    Global default value
    * @return The effective value of the parameter
    */
  private def getCmdOrConfigBoolean(cmdParameterOpt: Option[Boolean],
                                    configKey: String,
                                    defaultValue: Boolean): Boolean = {
    val enabled = cmdParameterOpt match {
      case Some(b) => b
      case None    =>
        if (conf.hasPath(configKey)) {
          conf.getBoolean(configKey)
        } else {
          defaultValue
        }
    }
    enabled
  }

  private def obtainSparkSession()(implicit cmd: ConfCmdConfig): SparkSession = {
    val enceladusVersion = ProjectMetadataTools.getEnceladusVersion
    log.info(s"Enceladus version $enceladusVersion")
    val reportVersion = cmd.reportVersion.map(_.toString).getOrElse("")
    val spark: SparkSession = SparkSession.builder()
      .appName(s"Dynamic Conformance $enceladusVersion ${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} $reportVersion")
      .getOrCreate()

    TimeZoneNormalizer.normalizeSessionTimeZone(spark)
    spark
  }

  private def inferVersion(hdfsPublishPath: String, reportDate: String)
                          (implicit fsUtils: FileSystemVersionUtils):Int = {
    val newVersion = fsUtils.getLatestVersion(hdfsPublishPath, reportDate) + 1
    log.warn(s"Report version not provided, inferred report version: $newVersion")
    log.warn("This is an EXPERIMENTAL feature.")
    log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
    log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
    newVersion
  }

  private def initFunctionalExtensions(reportVersion: Int, pathCfg: PathCfg)(implicit spark: SparkSession,
                                                                             dao: MenasDAO,
                                                                             cmd: ConfCmdConfig): Unit = {
    // Enable Spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // Enable Control Framework
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper
    spark.enableControlMeasuresTracking(s"${pathCfg.stdPath}/_INFO")
      .setControlMeasuresWorkflow("Conformance")

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)

    // Enable non-default persistence storage level if provided in the command line
    cmd.persistStorageLevel.foreach(Atum.setCachingStorageLevel)

    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(conf, cmd.datasetName, cmd.datasetVersion, cmd.reportDate, reportVersion)
  }

  private def initPerformanceMeasurer(stdPath: String)
                                     (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils): PerformanceMeasurer = {
    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val stdDirSize = fsUtils.getDirectorySize(stdPath)
    performance.startMeasurement(stdDirSize)
    performance
  }

  private def conform(conformance: Dataset, inputData: sql.Dataset[Row], enableCF: Boolean, recordIdGenerationStrategy: IdType)
                     (implicit spark: SparkSession, cmd: ConfCmdConfig, fsUtils: FileSystemVersionUtils, dao: MenasDAO): DataFrame = {
    implicit val featureSwitcher: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(isExperimentalRuleEnabled())
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled())
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(broadcastingStrategyMode)
      .setBroadcastMaxSizeMb(broadcastingMaxSizeMb)

    Try {
      handleControlInfoValidation()
      DynamicInterpreter.interpret(conformance, inputData)
    } match {
      case Failure(e: ValidationException) =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Conformance", e.getMessage, e.techDetails)
        throw e
      case Failure(NonFatal(e)) =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Conformance", e.getMessage, sw.toString)
        throw e
      case Success(conformedDF) =>
        if (SchemaUtils.fieldExists(Constants.EnceladusRecordId, conformedDF.schema)) {
          conformedDF // no new id regeneration
        } else {
          RecordIdGeneration.addRecordIdColumnByStrategy(conformedDF, Constants.EnceladusRecordId, recordIdGenerationStrategy)
        }

    }
  }

  private def processResult(result: DataFrame,
                            performance: PerformanceMeasurer,
                            pathCfg: PathCfg,
                            reportVersion: Int,
                            cmdLineArgs: String,
                            menasCredentials: MenasCredentials)
                           (implicit spark: SparkSession, cmd: ConfCmdConfig, fsUtils: FileSystemVersionUtils): Unit = {
    val withPartCols = result
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(cmd.reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(reportVersion))

    val recordCount = result.lastCheckpointRowCount match {
      case None    => withPartCols.count
      case Some(p) => p
    }
    if (recordCount == 0) { handleEmptyOutputAfterConformance() }

    // ensure the whole path but version exists
    fsUtils.createAllButLastSubDir(pathCfg.publishPath)

    withPartCols.write.parquet(pathCfg.publishPath)

    val publishDirSize = fsUtils.getDirectorySize(pathCfg.publishPath)
    performance.finishMeasurement(publishDirSize, recordCount)
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "conform",
      pathCfg.stdPath, pathCfg.publishPath, menasCredentials.username, cmdLineArgs)

    withPartCols.writeInfoFile(pathCfg.publishPath)
    cmd.performanceMetricsFile.foreach(fileName => {
      try {
        performance.writeMetricsToFile(fileName)
      } catch {
        case NonFatal(e) => log.error(s"Unable to write performance metrics to file '$fileName': ${e.getMessage}")
      }
    })

    if (isAutocleanStdFolderEnabled()) {
      fsUtils.deleteDirectoryRecursively(pathCfg.stdPath)
    }
  }

  private def handleEmptyOutputAfterConformance()(implicit spark: SparkSession): Unit = {
    import za.co.absa.atum.core.Constants._

    val areCountMeasurementsAllZero = Atum.getControlMeasure.checkpoints
      .flatMap(checkpoint =>
        checkpoint.controls.filter(control =>
          control.controlName.equalsIgnoreCase(controlTypeRecordCount)))
      .forall(m => Try(m.controlValue.toString.toDouble).toOption.contains(0D))

    if (areCountMeasurementsAllZero) {
      log.warn("Empty output after running Dynamic Conformance. Previous checkpoints show this is correct.")
    } else {
      val errMsg = "Empty output after running Dynamic Conformance, " +
        "while previous checkpoints show non zero record count"
      AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Standardization", errMsg, "")
      throw new IllegalStateException(errMsg)
    }
  }

  private def handleControlInfoValidation(): Unit = {
    ControlInfoValidation.addRawAndSourceRecordCountsToMetadata() match {
      case Failure(ex: za.co.absa.enceladus.utils.validation.ValidationException) => {
        val confEntry = "control.info.validation"
        conf.getString(confEntry) match {
          case "strict" => throw ex
          case "warning" => log.warn(ex.msg)
          case "none" =>
          case _ => throw new RuntimeException(s"Invalid $confEntry value")
        }
      }
      case Failure(ex) => throw ex
      case Success(_) =>
    }
  }

  def buildPublishPath(infoDateCol: String,
                       infoVersionCol: String,
                       cmd: ConfCmdConfig,
                       ds: Dataset,
                       reportVersion: Int): String = {
    (cmd.publishPathOverride, cmd.folderPrefix) match {
      case (None, None)                   =>
        s"${ds.hdfsPublishPath}/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
      case (None, Some(folderPrefix))     =>
        s"${ds.hdfsPublishPath}/$folderPrefix/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
      case (Some(publishPathOverride), _) =>
        publishPathOverride
    }
  }

  private final case class PathCfg(publishPath: String, stdPath: String)
}
