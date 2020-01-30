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
import org.apache.spark.sql
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.AtumImplicits.{DataSetWrapper, StringToPath}
import za.co.absa.enceladus.conformance.interpreter.{Always, DynamicInterpreter, FeatureSwitches, ThreeStateSwitch}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.general.ProjectMetadataTools
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import scala.util.Try
import scala.util.control.NonFatal

object StreamingConformanceJob {
  TimeZoneNormalizer.normalizeJVMTimeZone()

  private val infoDateColumn = "enceladus_info_date"
  private val infoDateColumnString = s"${infoDateColumn}_string"
  private val reportDateFormat = "yyyy-MM-dd"
  private val infoVersionColumn = "enceladus_info_version"

  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val conf: Config = ConfigFactory.load()
  private val menasBaseUrls = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))

  // scalastyle:off method.length
  def main(args: Array[String]) {
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
      publishPath = buildPublishPath(infoDateColumn, infoVersionColumn, cmd, conformance, reportVersion),
      stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"), cmd.datasetName,
        cmd.datasetVersion.toString, cmd.reportDate, reportVersion.toString)
    )
    log.info(s"stdpath = ${pathCfg.stdPath}, publishPath = ${pathCfg.publishPath}")
    // die before performing any computation if the output path already exists
    if (fsUtils.hdfsExists(pathCfg.publishPath)) {
      throw new IllegalStateException(
        s"Path ${pathCfg.publishPath} already exists. Increment the run version, or delete ${pathCfg.publishPath}")
    }

    // load data for input and mapping tables
    val df1 = spark.read.parquet(pathCfg.stdPath)
    val inputData = spark.readStream.schema(df1.schema).parquet(pathCfg.stdPath)

    implicit val featureSwitcher: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(isExperimentalRuleEnabled())
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled())
      .setControlFrameworkEnabled(false)
      .setBroadcastStrategyMode(Always)
      .setBroadcastMaxSizeMb(broadcastingMaxSizeMb)

    val result = DynamicInterpreter.interpret(conformance, inputData)

    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
    val withPartCols = result
      .withColumnIfDoesNotExist(infoDateColumn, to_date(lit(cmd.reportDate), reportDateFormat))
      .withColumnIfDoesNotExist(infoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(infoVersionColumn, lit(reportVersion))

    // ensure the whole path but version exists
    fsUtils.createAllButLastSubDir(pathCfg.publishPath)

    fsUtils.deleteDirectoryRecursively("/tmp/tmp-checkpoint")

    val streamingJob = withPartCols.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", pathCfg.publishPath)
      .option("checkpointLocation", "/tmp/tmp-checkpoint")
      .start()

    streamingJob.awaitTermination()
    log.info("Conformance finished successfully")
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
                          (implicit fsUtils: FileSystemVersionUtils): Int = {
    val newVersion = fsUtils.getLatestVersion(hdfsPublishPath, reportDate) + 1
    log.warn(s"Report version not provided, inferred report version: $newVersion")
    log.warn("This is an EXPERIMENTAL feature.")
    log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
    log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
    newVersion
  }

  private def initFunctionalExtensions()(implicit spark: SparkSession, dao: MenasDAO, cmd: ConfCmdConfig): Unit = {
    // Enable Spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()
  }

  private def initPerformanceMeasurer(stdPath: String)
                                     (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils): PerformanceMeasurer = {
    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val stdDirSize = fsUtils.getDirectorySize(stdPath)
    performance.startMeasurement(stdDirSize)
    performance
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
