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

package za.co.absa.enceladus.common

import java.text.MessageFormat
import java.time.Instant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.{Atum, ControlType}
import za.co.absa.enceladus.common.Constants.{InfoDateColumn, InfoDateColumnString, InfoVersionColumn, ReportDateFormat}
import za.co.absa.enceladus.common.config.{CommonConfConstants, JobConfigParser, PathConfig}
import za.co.absa.enceladus.common.plugin.PostProcessingService
import za.co.absa.enceladus.common.plugin.menas.{MenasPlugin, MenasRunUrl}
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.MenasConnectionStringParser
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams
import za.co.absa.enceladus.utils.general.ProjectMetadata
import za.co.absa.enceladus.utils.config.{ConfigReader, PathWithFs, SecureConfig}
import za.co.absa.enceladus.utils.fs.{FileSystemUtils, HadoopFsUtils}
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.modules.SourcePhase.Standardization
import za.co.absa.enceladus.common.performance.PerformanceMeasurer
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.enceladus.utils.validation.ValidationLevel
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait CommonJobExecution extends ProjectMetadata {
  protected case class PreparationResult(dataset: Dataset,
                                         reportVersion: Int,
                                         pathCfg: PathConfig,
                                         performance: PerformanceMeasurer)

  TimeZoneNormalizer.normalizeJVMTimeZone()
  SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  protected val configReader: ConfigReader = new ConfigReader()
  protected val menasBaseUrls: List[String] = MenasConnectionStringParser.parse(configReader.getString("menas.rest.uri"))
  protected val menasUrlsRetryCount: Option[Int] = configReader.getIntOption("menas.rest.retryCount")
  protected val menasSetup: String = configReader.getString("menas.rest.availability.setup")
  protected var secureConfig: Map[String, String] = Map.empty

  protected def obtainSparkSession[T](jobName: String)(implicit cmd: JobConfigParser[T]): SparkSession = {
    val enceladusVersion = projectVersion
    log.info(s"Enceladus version $enceladusVersion")
    val reportVersion = cmd.reportVersion.map(_.toString).getOrElse("")

    // ssl paths stripped paths for current directory usage (expecting files distributed via spark-submit's "--files"
    val executorSecConfig = SecureConfig.getSslProperties(configReader.config, useCurrentDirectoryPaths = true)

    val spark = SparkSession.builder()
      .appName(s"$jobName $enceladusVersion ${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} $reportVersion")
      .config("spark.executor.extraJavaOptions", SecureConfig.javaOptsStringFromConfigMap(executorSecConfig)) // system properties on executors
      .getOrCreate()
    TimeZoneNormalizer.normalizeSessionTimeZone(spark)
    spark
  }

  protected def initialValidation(): Unit = {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    val secConf = SecureConfig.getSslProperties(configReader.config)
    SecureConfig.setSystemProperties(secConf)
  }

  protected def prepareJob[T]()
                             (implicit dao: MenasDAO,
                              cmd: JobConfigParser[T],
                              spark: SparkSession): PreparationResult = {
    configReader.logEffectiveConfigProps(Constants.ConfigKeysToRedact)
    dao.authenticate()

    implicit val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration

    val dataset = dao.getDataset(cmd.datasetName, cmd.datasetVersion, ValidationLevel.ForRun)
    dataset.propertiesValidation match {
      case Some(validation) if !validation.isValid =>
        throw new IllegalStateException("Dataset validation failed, errors found in fields:\n" +
          validation.errors.map { case (field, errMsg) => s" - '$field': $errMsg" }.mkString("\n")
        )
      case Some(validation) if validation.nonEmpty =>
        val warning = validation.warnings.map { case (field, warnMsg) =>
          val header = s" - '$field': "
          s"$header${warnMsg.mkString(s"\n$header")}"
        }.mkString("\n")
        log.warn("Dataset validation had some warnings:\n" + warning)
      case None => throw new IllegalStateException("Dataset validation was not retrieved correctly")
      case _ => // no problems found
    }

    val minPartition = configReader.getLongOption(CommonConfConstants.minPartitionSizeKey)
    val maxPartition = configReader.getLongOption(CommonConfConstants.maxPartitionSizeKey)

    (minPartition, maxPartition) match {
      case (Some(min), Some(max)) if min >= max => throw new IllegalStateException(
        s"${CommonConfConstants.minPartitionSizeKey} has to be smaller than ${CommonConfConstants.maxPartitionSizeKey}"
      )
      case _ => //validation passed
    }

    val reportVersion = getReportVersion(cmd, dataset)
    val pathCfg: PathConfig = getPathConfig(cmd, dataset, reportVersion)

    validatePaths(pathCfg)

    // Enable Spline
    import za.co.absa.spline.harvester.SparkLineageInitializer._
    spark.enableLineageTracking()

    // Enable non-default persistence storage level if provided in the command line
    cmd.persistStorageLevel.foreach(Atum.setCachingStorageLevel)

    PreparationResult(dataset, reportVersion, pathCfg, new PerformanceMeasurer(spark.sparkContext.appName))
  }

  protected def validatePaths(pathConfig: PathConfig): Unit

  protected def validateIfOutputPathAlreadyExists(entry: PathWithFs): Unit = {
    val fsUtils = HadoopFsUtils.getOrCreate(entry.fileSystem)
    if (fsUtils.exists(entry.path)) {
      throw new IllegalStateException(
        s"Path ${entry.path} already exists. Increment the run version, or delete ${entry.path}"
      )
    }
  }

  protected def validateInputPath(entry: PathWithFs): Unit = {
    val fsUtils = HadoopFsUtils.getOrCreate(entry.fileSystem)
    if (!fsUtils.exists(entry.path)) {
      throw new IllegalStateException(
        s"Input path ${entry.path} does not exist"
      )
    }
  }

  /**
   * Post processing rereads the data from a path on FS (based on `sourcePhase`)
   */
  protected def runPostProcessing[T](sourcePhase: SourcePhase, preparationResult: PreparationResult, jobCmdConfig: JobConfigParser[T])
                                    (implicit spark: SparkSession): Unit = {
    val outputPath = sourcePhase match {
      case Standardization => preparationResult.pathCfg.standardization.path
      case _ => preparationResult.pathCfg.publish.path
    }

    log.info(s"rereading outputPath $outputPath to run postProcessing")
    val df = spark.read.parquet(outputPath)
    val runId = MenasPlugin.runNumber

    // reporting the UI url(s) - if more than one, its comma-separated
    val runUrl: Option[String] = runId.map { runNumber =>
      menasBaseUrls.map { menasBaseUrl =>
        MenasRunUrl.getMenasUiRunUrl(menasBaseUrl, jobCmdConfig.datasetName, jobCmdConfig.datasetVersion, runNumber)
      }.mkString(",")
    }

    val sourceSystem = Atum.getControlMeasure.metadata.sourceApplication
    val uniqueRunId = Atum.getControlMeasure.runUniqueId

    val params = ErrorSenderPluginParams(jobCmdConfig.datasetName,
      jobCmdConfig.datasetVersion, jobCmdConfig.reportDate, preparationResult.reportVersion, outputPath,
      sourcePhase, sourceSystem, runUrl, runId, uniqueRunId, Instant.now)
    val postProcessingService = PostProcessingService(configReader.config, params)
    postProcessingService.onSaveOutput(df)

    if (runId.isEmpty) {
      log.warn("No run number found, the Run URL cannot be properly reported!")
    }
  }

  protected def repartitionDataFrame(df: DataFrame, minBlockSize: Option[Long], maxBlockSize: Option[Long])
                                    (implicit spark: SparkSession): DataFrame = {
    def computeBlockCount(desiredBlockSize: Long, totalByteSize: BigInt, addRemainder: Boolean): Int = {
      val int = (totalByteSize / desiredBlockSize).toInt
      val blockCount = int + (if (addRemainder && (totalByteSize % desiredBlockSize != 0)) 1 else 0)
      blockCount max 1
    }

    def changePartitionCount(blockCount: Int, fnc: Int => DataFrame): DataFrame = {
      val outputDf = fnc(blockCount)
      log.info(s"Number of output partitions: ${outputDf.rdd.getNumPartitions}")
      outputDf
    }

    val currentPartionCount = df.rdd.getNumPartitions

    if (currentPartionCount > 0) {
      val catalystPlan = df.queryExecution.logical
      val sizeInBytes = spark.sessionState.executePlan(catalystPlan).optimizedPlan.stats.sizeInBytes

      val currentBlockSize = sizeInBytes / df.rdd.getNumPartitions

      (minBlockSize, maxBlockSize) match {
        case (Some(min), None) if currentBlockSize < min =>
          changePartitionCount(computeBlockCount(min, sizeInBytes, addRemainder = false), df.coalesce)
        case (None, Some(max)) if currentBlockSize > max =>
          changePartitionCount(computeBlockCount(max, sizeInBytes, addRemainder = true), df.repartition)
        case (Some(min), Some(max)) if currentBlockSize < min || currentBlockSize > max =>
          changePartitionCount(computeBlockCount(max, sizeInBytes, addRemainder = true), df.repartition)
        case _ => df
      }
    } else {
      // empty dataframe
      df
    }
  }

  protected def finishJob[T](jobConfig: JobConfigParser[T]): Unit = {
    val name = jobConfig.datasetName
    val version = jobConfig.datasetVersion
    MenasPlugin.runNumber.foreach { runNumber =>
      menasBaseUrls.foreach { menasBaseUrl =>
        val apiUrl = MenasRunUrl.getMenasApiRunUrl(menasBaseUrl, name, version, runNumber)
        val uiUrl = MenasRunUrl.getMenasUiRunUrl(menasBaseUrl, name, version, runNumber)

        log.info(s"Menas API Run URL: $apiUrl")
        log.info(s"Menas UI Run URL: $uiUrl")
      }
    }
  }

  protected def getPathConfig[T](cmd: JobConfigParser[T], dataset: Dataset, reportVersion: Int)
                                (implicit hadoopConf: Configuration): PathConfig = {

    val rawPath = buildRawPath(cmd, dataset, reportVersion)
    val publishPath = buildPublishPath(cmd, dataset, reportVersion)
    val standardizationPath = getStandardizationPath(cmd, reportVersion)

    PathConfig.fromPaths(rawPath, publishPath, standardizationPath)
  }

  private def buildPublishPath[T](cmd: JobConfigParser[T], ds: Dataset, reportVersion: Int): String = {
    val infoDateCol: String = InfoDateColumn
    val infoVersionCol: String = InfoVersionColumn

    cmd.folderPrefix match {
      case None => s"${ds.hdfsPublishPath}/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
      case Some(folderPrefix) =>
        s"${ds.hdfsPublishPath}/$folderPrefix/$infoDateCol=${cmd.reportDate}/$infoVersionCol=$reportVersion"
    }
  }

  private def buildRawPath[T](cmd: JobConfigParser[T], dataset: Dataset, reportVersion: Int): String = {
    val dateTokens = cmd.reportDate.split("-")
    val folderSuffix = s"/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v$reportVersion"
    cmd.folderPrefix match {
      case None => s"${dataset.hdfsPath}$folderSuffix"
      case Some(folderPrefix) => s"${dataset.hdfsPath}/$folderPrefix$folderSuffix"
    }
  }

  private def getStandardizationPath[T](jobConfig: JobConfigParser[T], reportVersion: Int): String = {
    MessageFormat.format(configReader.getString("standardized.hdfs.path"),
      jobConfig.datasetName,
      jobConfig.datasetVersion.toString,
      jobConfig.reportDate,
      reportVersion.toString)
  }

  protected def handleControlInfoValidation(): Unit = {
    ControlInfoValidation.addRawAndSourceRecordCountsToMetadata() match {
      case Failure(ex: za.co.absa.enceladus.utils.validation.ValidationException) =>
        val confEntry = "control.info.validation"
        configReader.getString(confEntry) match {
          case "strict" => throw ex
          case "warning" => log.warn(ex.msg)
          case "none" =>
          case _ => throw new RuntimeException(s"Invalid $confEntry value")
        }
      case Failure(ex) => throw ex
      case Success(_) =>
    }
  }

  protected def writePerformanceMetrics[T](performance: PerformanceMeasurer, jobCmdConfig: JobConfigParser[T]): Unit = {
    jobCmdConfig.performanceMetricsFile.foreach(fileName => try {
      performance.writeMetricsToFile(fileName)
    } catch {
      case NonFatal(e) => log.error(s"Unable to write performance metrics to file '$fileName': ${e.getMessage}")
    })
  }

  protected def addCustomDataToInfoFile(conf: ConfigReader, data: Map[String, String]): Unit = {
    val keyPrefix = Try {
      conf.getString("control.info.dataset.properties.prefix")
    }.toOption.getOrElse("")

    log.debug(s"Writing custom data to info file (with prefix '$keyPrefix'): $data")
    data.foreach { case (key, value) =>
      Atum.setAdditionalInfo((s"$keyPrefix$key", value))
    }
  }

  protected def handleEmptyOutput(job: SourcePhase)(implicit spark: SparkSession): Unit = {

    val areCountMeasurementsAllZero = Atum.getControlMeasure.checkpoints
      .flatMap(checkpoint =>
        checkpoint.controls.filter(control =>
          ControlType.isControlMeasureTypeEqual(control.controlType, ControlType.Count.value)
        )
      )
      .forall(m => Try(m.controlValue.toString.toDouble).toOption.contains(0D))

    if (areCountMeasurementsAllZero) {
      log.warn(s"Empty output after running $job. Previous checkpoints show this is correct.")
    } else {
      val errMsg = s"Empty output after running $job, while previous checkpoints show non zero record count"
      spark.setControlMeasurementError(job.toString, errMsg, "")
      throw new IllegalStateException(errMsg)
    }
  }

  protected def addInfoColumns(intoDf: DataFrame, reportDate: String, reportVersion: Int): DataFrame = {
    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
    intoDf
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(reportVersion))
  }

  private def getReportVersion[T](jobConfig: JobConfigParser[T], dataset: Dataset)(implicit hadoopConf: Configuration): Int = {
    jobConfig.reportVersion match {
      case Some(version) => version
      case None =>

        // Since `pathConfig.publish.fileSystem` is not available at this point yet, a temporary publish-FS is create & used here instead
        val tempPublishFs: FileSystem = FileSystemUtils.getFileSystemFromPath(dataset.hdfsPublishPath)
        val fsUtils = HadoopFsUtils.getOrCreate(tempPublishFs)
        val newVersion = fsUtils.getLatestVersion(dataset.hdfsPublishPath, jobConfig.reportDate) + 1
        log.warn(s"Report version not provided, inferred report version: $newVersion")
        log.warn("This is an EXPERIMENTAL feature.")
        log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
        log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
        newVersion
    }
  }
}
