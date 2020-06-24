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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.common.config.JobConfig
import za.co.absa.enceladus.common.plugin.PostProcessingService
import za.co.absa.enceladus.common.plugin.menas.{MenasPlugin, MenasRunUrl}
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.MenasConnectionStringParser
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams
import za.co.absa.enceladus.plugins.builtin.utils.SecureKafka
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.general.ProjectMetadataTools
import za.co.absa.enceladus.utils.modules.SourceId
import za.co.absa.enceladus.utils.performance.PerformanceMeasurer
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait CommonJobExecution {

  protected case class PreparationResult(
                                          dataset: Dataset,
                                          reportVersion: Int,
                                          pathCfg: PathConfig,
                                          performance: PerformanceMeasurer
                                        )

  TimeZoneNormalizer.normalizeJVMTimeZone()
  SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  protected val conf: Config = ConfigFactory.load()
  protected val menasBaseUrls: List[String] = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))

  protected def obtainSparkSession[T]()(implicit cmd: JobConfig[T]): SparkSession = {
    val enceladusVersion = ProjectMetadataTools.getEnceladusVersion
    log.info(s"Enceladus version $enceladusVersion")
    val reportVersion = cmd.reportVersion.map(_.toString).getOrElse("")
    val spark = SparkSession.builder()
      .appName(s"Standardisation $enceladusVersion ${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} $reportVersion")
      .getOrCreate()
    TimeZoneNormalizer.normalizeSessionTimeZone(spark)
    spark
  }

  protected def initialValidation(): Unit = {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    SecureKafka.setSecureKafkaProperties(conf)
  }

  protected def prepareJob[T]()
                             (implicit dao: MenasDAO,
                              cmd: JobConfig[T],
                              fsUtils: FileSystemVersionUtils,
                              spark: SparkSession): PreparationResult = {
    dao.authenticate()
    val dataset = dao.getDataset(cmd.datasetName, cmd.datasetVersion)
    val reportVersion = getReportVersion(cmd, dataset)
    val pathCfg = getPathCfg(cmd, dataset, reportVersion)

    log.info(s"input path: ${pathCfg.inputPath}")
    log.info(s"output path: ${pathCfg.outputPath}")
    // die if the output path exists
    validateForExistingOutputPath(fsUtils, pathCfg)

    val performance = initPerformanceMeasurer(pathCfg.inputPath)

    // Enable Spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)

    // Enable non-default persistence storage level if provided in the command line
    cmd.persistStorageLevel.foreach(Atum.setCachingStorageLevel)

    PreparationResult(dataset, reportVersion, pathCfg, performance)
  }

  protected def runPostProcessing[T](sourceId: SourceId, preparationResult: PreparationResult, jobCmdConfig: JobConfig[T])
                                    (implicit spark: SparkSession, fileSystemVersionUtils: FileSystemVersionUtils): Unit = {
    val df = spark.read.parquet(preparationResult.pathCfg.outputPath)
    val runId = MenasPlugin.runNumber

    if (runId.isEmpty) {
      log.warn("No run number found, the Run URL cannot be properly reported!")
    }

    // reporting the UI url(s) - if more than one, its comma-separated
    val runUrl: Option[String] = runId.map { runNumber =>
      menasBaseUrls.map { menasBaseUrl =>
        MenasRunUrl.getMenasUiRunUrl(menasBaseUrl, jobCmdConfig.datasetName, jobCmdConfig.datasetVersion, runNumber)
      }.mkString(",")
    }

    val sourceSystem = Atum.getControlMeasure.metadata.sourceApplication
    val uniqueRunId = Atum.getControlMeasure.runUniqueId

    val params = ErrorSenderPluginParams(jobCmdConfig.datasetName,
      jobCmdConfig.datasetVersion, jobCmdConfig.reportDate, preparationResult.reportVersion, preparationResult.pathCfg.outputPath,
      sourceId, sourceSystem, runUrl, runId, uniqueRunId, Instant.now)
    val postProcessingService = PostProcessingService(conf, params)
    postProcessingService.onSaveOutput(df)
  }

  protected def finishJob[T](jobConfig: JobConfig[T]): Unit = {
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


  protected def getPathCfg[T](cmd: JobConfig[T], dataset: Dataset, reportVetsion: Int): PathConfig

  protected def getStandardizationPath[T](jobConfig: JobConfig[T], reportVersion: Int): String = {
    MessageFormat.format(conf.getString("standardized.hdfs.path"),
      jobConfig.datasetName,
      jobConfig.datasetVersion.toString,
      jobConfig.reportDate,
      reportVersion.toString)
  }

  protected def handleControlInfoValidation(): Unit = {
    ControlInfoValidation.addRawAndSourceRecordCountsToMetadata() match {
      case Failure(ex: za.co.absa.enceladus.utils.validation.ValidationException) =>
        val confEntry = "control.info.validation"
        conf.getString(confEntry) match {
          case "strict" => throw ex
          case "warning" => log.warn(ex.msg)
          case "none" =>
          case _ => throw new RuntimeException(s"Invalid $confEntry value")
        }
      case Failure(ex) => throw ex
      case Success(_) =>
    }
  }

  protected def validateForExistingOutputPath(fsUtils: FileSystemVersionUtils, pathCfg: PathConfig): Unit = {
    if (fsUtils.hdfsExists(pathCfg.outputPath)) {
      throw new IllegalStateException(
        s"Path ${pathCfg.outputPath} already exists. Increment the run version, or delete ${pathCfg.outputPath}"
      )
    }
  }

  protected def writePerformanceMetrics[T](performance: PerformanceMeasurer, jobCmdConfig: JobConfig[T]): Unit = {
    jobCmdConfig.performanceMetricsFile.foreach(fileName => try {
      performance.writeMetricsToFile(fileName)
    } catch {
      case NonFatal(e) => log.error(s"Unable to write performance metrics to file '$fileName': ${e.getMessage}")
    })
  }

  protected def handleEmptyOutput(job: SourceId)(implicit spark: SparkSession): Unit = {
    import za.co.absa.atum.core.Constants._

    val areCountMeasurementsAllZero = Atum.getControlMeasure.checkpoints
      .flatMap(checkpoint =>
        checkpoint.controls.filter(control =>
          control.controlName.equalsIgnoreCase(controlTypeRecordCount)))
      .forall(m => Try(m.controlValue.toString.toDouble).toOption.contains(0D))

    if (areCountMeasurementsAllZero) {
      log.warn(s"Empty output after running $job. Previous checkpoints show this is correct.")
    } else {
      val errMsg = s"Empty output after running $job, while previous checkpoints show non zero record count"
      AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(job.toString, errMsg, "")
      throw new IllegalStateException(errMsg)
    }
  }

  private def getReportVersion[T](jobConfig: JobConfig[T], dataset: Dataset)(implicit fsUtils: FileSystemVersionUtils): Int = {
    jobConfig.reportVersion match {
      case Some(version) => version
      case None =>
        val newVersion = fsUtils.getLatestVersion(dataset.hdfsPublishPath, jobConfig.reportDate) + 1
        log.warn(s"Report version not provided, inferred report version: $newVersion")
        log.warn("This is an EXPERIMENTAL feature.")
        log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
        log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
        newVersion
    }
  }

  private def initPerformanceMeasurer(path: String)
                                     (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils): PerformanceMeasurer = {
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val stdDirSize = fsUtils.getDirectorySize(path)
    performance.startMeasurement(stdDirSize)
    performance
  }
}
