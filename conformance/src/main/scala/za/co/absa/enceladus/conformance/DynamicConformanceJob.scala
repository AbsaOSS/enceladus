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

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import za.co.absa.enceladus.dao.{EnceladusRestDAO, LoggedInUserInfo}
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.atum.AtumImplicits._
import com.typesafe.config.{Config, ConfigFactory}
import java.text.MessageFormat

import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import java.text.SimpleDateFormat

import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.utils.performance.PerformanceMeasurer

import scala.util.control.NonFatal
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.utils.menas.MenasPlugin
import za.co.absa.enceladus.model.Dataset

object DynamicConformanceJob {

  val log: Logger = LogManager.getLogger("enceladus.conformance.DynamicConformanceJob")
  val conf: Config = ConfigFactory.load()
  
  /** This method adds performance metrics to the _INFO file metadata **/
  def addPerformanceMetadata(spark: SparkSession, publishDirSize: Long, outputPath: String): Unit = {
    // Enceladus version
    val enceladusVersion = conf.getString("enceladus.version")
    Atum.setAdditionalInfo("conform_enceladus_version" -> enceladusVersion)

    // Spark job configuration
    val sc = spark.sparkContext
    // The number of executors minus the driver
    val numberOfExecutrs = sc.getExecutorMemoryStatus.keys.size - 1
    val executorMemory = spark.sparkContext.getConf.get("spark.executor.memory")
    Atum.setAdditionalInfo("conform_application_id" -> spark.sparkContext.applicationId)
    Atum.setAdditionalInfo("conform_executors_num" -> s"$numberOfExecutrs")
    Atum.setAdditionalInfo("conform_executors_memory" -> s"$executorMemory")
    Atum.setAdditionalInfo("publish_dir_size" -> publishDirSize.toString)

    // Calculate the number of errors
    import spark.implicits._

    val df = spark.read.parquet(outputPath)
    val numRecordsFailed = df
      .filter(size($"errCol")>0).count
    val numRecordsSuccessful = df
      .filter(size($"errCol")===0).count
    val numOfErrors = df
      .withColumn("enceladus_error_count", size($"errCol")).agg(sum($"enceladus_error_count"))
      .take(1)(0)(0).toString.toLong

    Atum.setAdditionalInfo("conform_records_succeeded" -> numRecordsSuccessful.toString)
    Atum.setAdditionalInfo("conform_records_failed" -> numRecordsFailed.toString)
    Atum.setAdditionalInfo("conform_errors_count" -> numOfErrors.toString)
    Atum.setAdditionalInfo("conform_username" -> LoggedInUserInfo.getUserName)

    if (numRecordsSuccessful == 0) {
      log.error("No successful records after running Dynamic Conformance. Possibly some of the conformance rules are incorrectly defined for the dataset.")
    }
  }


  def main(args: Array[String]) {

    val infoDateColumn = "enceladus_info_date"
    val infoVersionColumn = "enceladus_info_version"

    implicit val cmd = CmdConfig.getCmdLineArguments(args)

    // initialize spark
    implicit val spark = SparkSession.builder()
      .appName(s"Dynamic Conformance - ${args(0)} (${args(1)}) - ${cmd.reportDate} - ${cmd.reportVersion}")
      .getOrCreate()
    import spark.implicits._

    spark.enableControlMeasuresTracking()
      .setControlMeasuresWorkflow("Conformance")

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)

    MenasPlugin.enableMenas()

    // enable Spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // use REST DAO
    implicit val dao = EnceladusRestDAO
    implicit val enableCF = true

    // get the dataset definition
    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion)
    
    val dateTokens = cmd.reportDate.split("-")
    val stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"), cmd.datasetName, cmd.datasetVersion.toString, cmd.reportDate, cmd.reportVersion.toString)

    val publishPath: String = buildPublishPath(infoDateColumn, infoVersionColumn, cmd, conformance)

    log.info(s"stdpath = $stdPath, publishPath = $publishPath")

    // die before performing any computation if the output path already exists
    if (FileSystemVersionUtils.exists(publishPath)) throw new IllegalStateException(s"Path $publishPath already exists. Increment the run version, or delete $publishPath")

    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val stdDirSize = FileSystemVersionUtils.getDirectorySize(stdPath)
    performance.startMeasurement(stdDirSize)

    // load data for input and mapping tables
    val inputData = DataSource.getData(stdPath, dateTokens(0), dateTokens(1), dateTokens(2), "")

    // perform the conformance
    val result = try {
      DynamicInterpreter.interpret(conformance, inputData)
    }
    catch {
      case e: ValidationException =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Conformance", e.getMessage, e.techDetails)
        throw e
      case NonFatal(e) =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Conformance", e.getMessage, sw.toString)
        throw e
    }

    val format = new SimpleDateFormat("yyyy-MM-dd")

    val withPartCols = result.withColumn(infoDateColumn, lit(new java.sql.Date(format.parse(cmd.reportDate).getTime)))
      .withColumn(infoVersionColumn, lit(cmd.reportVersion))

    val recordCount = result.lastCheckpointRowCount match {
      case None => withPartCols.count
      case Some(p) => p
    }

    if (recordCount == 0) {
      val errMsg = "Empty output after running Dynamic Conformance."
      AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Conformance", errMsg, "")
      throw new IllegalStateException(errMsg)
    }

    // ensure the whole path but version exists
    FileSystemVersionUtils.createAllButLastSubDir(publishPath)

    withPartCols.write.parquet(publishPath)

    val publishDirSize = FileSystemVersionUtils.getDirectorySize(publishPath)
    performance.finishMeasurement(publishDirSize, recordCount)
    addPerformanceMetadata(spark, publishDirSize, publishPath)
    cmd.performanceMetricsFile.foreach(fileName => {
      try {
        performance.writeMetricsToFile(fileName)
      } catch {
        case NonFatal(_) => log.error(s"Unable to write performance metrics to file '$fileName'")
      }
    })
  }

  def buildPublishPath(infoDateColumn: String, infoVersionColumn: String, cmd: CmdConfig, conformance: Dataset): String = {
    cmd.publishPathOverride match {
      case None => cmd.folderPrefix match {
        case None => s"${conformance.hdfsPublishPath}/$infoDateColumn=${cmd.reportDate}/$infoVersionColumn=${cmd.reportVersion}"
        case Some(folderPrefix) => s"${conformance.hdfsPublishPath}/$folderPrefix/$infoDateColumn=${cmd.reportDate}/$infoVersionColumn=${cmd.reportVersion}"
      }
      case Some(publishPathOverride) => publishPathOverride
    }
  }
}