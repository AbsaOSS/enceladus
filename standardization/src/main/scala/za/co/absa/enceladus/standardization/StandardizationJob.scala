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

package za.co.absa.enceladus.standardization

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.validation.ValidationException
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.dao.{EnceladusRestDAO, LoggedInUserInfo}
import com.typesafe.config.{Config, ConfigFactory}
import java.text.MessageFormat
import java.util.UUID

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.PerformanceMeasurer

import scala.util.control.NonFatal
import org.apache.spark.sql.functions.{size, sum}
import za.co.absa.atum.AtumImplicits
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.menas.MenasPlugin

object StandardizationJob {

  val log: Logger = LogManager.getLogger("enceladus.standardization.StandardizationJob")
  val conf: Config = ConfigFactory.load()

  /** This method adds performance metrics to the _INFO file metadata **/
  def addPerformanceMetadata(spark: SparkSession, rawDirSize: Long, stdDirSize: Long, outputPath: String): Unit = {
    // Enceladus version
    val enceladusVersion = conf.getString("enceladus.version")
    Atum.setAdditionalInfo("std_enceladus_version" -> enceladusVersion)

    // Spark job configuration
    val sc = spark.sparkContext
    sc.getConf.getAll.mkString("\n")
    // The number of executors minus the driver
    val numberOfExecutrs = sc.getExecutorMemoryStatus.keys.size - 1
    val executorMemory = spark.sparkContext.getConf.get("spark.executor.memory")
    Atum.setAdditionalInfo("std_application_id" -> spark.sparkContext.applicationId)
    Atum.setAdditionalInfo("std_executors_num" -> s"$numberOfExecutrs")
    Atum.setAdditionalInfo("std_executors_memory" -> s"$executorMemory")

    // Directory sizes
    if (rawDirSize > 0) {
      val percent = (stdDirSize.toDouble / rawDirSize.toDouble) * 100
      val percentFormatted = f"$percent%3.2f"
      Atum.setAdditionalInfo("std_size_ratio" -> s"$percentFormatted %")
    }
    Atum.setAdditionalInfo("raw_dir_size" -> rawDirSize.toString)
    Atum.setAdditionalInfo("std_dir_size" -> stdDirSize.toString)

    // Calculate the number of errors
    import spark.implicits._

    val df = spark.read.parquet(outputPath)
    val numRecordsFailed = df
      .filter(size($"errCol") > 0).count
    val numRecordsSuccessful = df
      .filter(size($"errCol") === 0).count
    val numOfErrors = df
      .withColumn("enceladus_error_count", size($"errCol")).agg(sum($"enceladus_error_count"))
      .take(1)(0)(0).toString.toLong

    Atum.setAdditionalInfo("std_records_succeeded" -> numRecordsSuccessful.toString)
    Atum.setAdditionalInfo("std_records_failed" -> numRecordsFailed.toString)
    Atum.setAdditionalInfo("std_errors_count" -> numOfErrors.toString)
    Atum.setAdditionalInfo("std_username" -> LoggedInUserInfo.getUserName)

    if (numRecordsSuccessful == 0) {
      log.error("No successful records after running Standardization. Possibly the schema is incorrectly defined for the dataset.")
    }
  }

  def main(args: Array[String]) {

    val cmd = CmdConfig.getCmdLineArguments(args)

    implicit val spark = SparkSession.builder()
      .appName(s"Standardisation ${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} ${cmd.reportVersion}")
      .config("spark.sql.codegen.wholeStage", false) //disable whole stage code gen - the plan is too long
      .getOrCreate()

    import spark.implicits._

    implicit val udfLib = new UDFLibrary

    val dataset = EnceladusRestDAO.getDataset(cmd.datasetName, cmd.datasetVersion)

    val schema = EnceladusRestDAO.getSchema(dataset.schemaName, dataset.schemaVersion)
    val inputSchema = PlainSchemaGenerator.generateInputSchema(schema).asInstanceOf[StructType]
    val dateTokens = cmd.reportDate.split("-")
    val path: String = buildRawPath(cmd, dataset, dateTokens)
    println(s"input path: $path")
    val stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"), cmd.datasetName, cmd.datasetVersion.toString, cmd.reportDate, cmd
      .reportVersion.toString)
    println(s"output path: $stdPath")

    // die if the output path exists
    if (FileSystemVersionUtils.exists(stdPath)) throw new IllegalStateException(s"Path $stdPath already exists. Increment the run version, or " +
      s"delete $stdPath")

    // init CF
    spark.enableControlMeasuresTracking(s"$path/_INFO")
      .setControlMeasuresWorkflow("Standardization")

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)

    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(cmd.datasetName, cmd.datasetVersion, isJobStageOnly = true)

    // init spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)

    val dfReader = spark.read.format(cmd.rawFormat)

    // applying format specific options
    val  dfReader4= {
      val dfReader1 = if (cmd.rowTag.isDefined) dfReader.option("rowTag", cmd.rowTag.get) else dfReader
      val dfReader2 = if (cmd.csvDelimiter.isDefined) dfReader1.option("delimiter", cmd.csvDelimiter.get) else dfReader1
      val dfReader3 = if (cmd.csvHeader.isDefined) dfReader2.option("header", cmd.csvHeader.get) else dfReader2
      dfReader3
    }
    val dfReaderConfigured= if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      val dfReader5=if (cmd.fixedWidthTrimValues.get) dfReader4.option("trimValues", "true") else dfReader4
      dfReader5
    } else dfReader4

    val dfWithSchema = (if (!cmd.rawFormat.equalsIgnoreCase("parquet") ) {
      dfReaderConfigured.schema(inputSchema)
    } else {
      dfReaderConfigured
    }).load(s"$path/*")

    val dfAll = ensureSplittable(dfWithSchema, path, schema)

    val rawDirSize = FileSystemVersionUtils.getDirectorySize(path)

    performance.startMeasurement(rawDirSize)

    val std = try {
      StandardizationInterpreter.standardize(dfAll, schema, cmd.rawFormat)
    }
    catch {
      case e@ValidationException(msg, errors) =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Schema Validation", s"$msg\nDetails: ${
          errors.mkString("\n")
        }", "")
        throw e
      case NonFatal(e) if !e.isInstanceOf[ValidationException] =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Standardization", e.getMessage, sw.toString)
        throw e
    }

    // If the meta data value sourcecolumn is set override source data column name with the field name
    val stdRenameSourceColumns = schema.foldLeft(std)((std, field) => renameSourceColumns(std, field, true))

    stdRenameSourceColumns.setCheckpoint("Standardization Finish", persistInDatabase = false)

    val recordCount = stdRenameSourceColumns.lastCheckpointRowCount match {
      case None => std.count
      case Some(p) => p
    }

    if (recordCount == 0) {
      val errMsg = "Empty output after running Standardization."
      AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Standardization", errMsg, "")
      throw new IllegalStateException(errMsg)
    }
    stdRenameSourceColumns.write.parquet(stdPath)

    // Store performance metrics (record count, directory sizes, elapsed time, etc. to _INFO file metadata and performance file)
    val stdDirSize = FileSystemVersionUtils.getDirectorySize(stdPath)
    performance.finishMeasurement(stdDirSize, recordCount)
    cmd.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    if (cmd.csvDelimiter.isDefined) cmd.csvDelimiter.foreach(deLimiter => Atum.setAdditionalInfo("csv_delimiter" -> deLimiter))
    addPerformanceMetadata(spark, rawDirSize, stdDirSize, stdPath)
    stdRenameSourceColumns.writeInfoFile(stdPath)
    cmd.performanceMetricsFile.foreach(fileName => {
      try {
        performance.writeMetricsToFile(fileName)
      } catch {
        case NonFatal(_) => log.error(s"Unable to write performance metrics to file '$fileName'")
      }
    })
  }

  private def ensureSplittable(df: DataFrame, path: String, schema: StructType)(implicit spark: SparkSession) = {
    if (FileSystemVersionUtils.isNonSplittable(path)) {
      convertToSplittable(df, path, schema)
    } else {
      df
    }
  }

  private def convertToSplittable(df: DataFrame, path: String, schema: StructType)(implicit spark: SparkSession) = {
    log.warn("Dataset is stored in a non-splittable format. This can have a severe performance impact.")

    val tempParquetDir = s"/tmp/nonsplittable-to-parquet-${UUID.randomUUID()}"
    log.warn(s"Converting to Parquet in temporary dir: $tempParquetDir")

    // Handle renaming of source columns in case there are columns that will break because of issues in column names like spaces
    schema.foldLeft(df)((df, field) => renameSourceColumns(df, field, false)).write.parquet(tempParquetDir)

    FileSystemVersionUtils.deleteOnExit(tempParquetDir)
    // Reload from temp parquet and reverse column renaming above
    schema.foldLeft(spark.read.parquet(tempParquetDir))(reverseRenameSourceColumns)
  }

  private def renameSourceColumns(df: DataFrame, field: StructField, registerWithATUM: Boolean): DataFrame = {
    val renameDf = if (field.metadata.contains("sourcecolumn")) {
      log.info(s"schema field : ${field.name} : rename : ${field.metadata.getString("sourcecolumn")}")
      df.withColumnRenamed(field.metadata.getString("sourcecolumn"), field.name) //rename column in DF
    } else df

    if (registerWithATUM && field.metadata.contains("sourcecolumn"))
      renameDf.registerColumnRename(field.metadata.getString("sourcecolumn"), field.name) // register rename with ATUM
        // Re-add meta data
        .withColumn(field.name, renameDf.col(field.name).as("", field.metadata))
    else renameDf
  }

  private def reverseRenameSourceColumns(df: DataFrame, field: StructField): DataFrame = {
    if (field.metadata.contains("sourcecolumn")) {
      log.info(s"schema field : ${field.metadata.getString("sourcecolumn")} : reverse rename : ${field.name}")
      df.withColumnRenamed(field.name, field.metadata.getString("sourcecolumn")) // reverse rename column in DF
    } else {
      df
    }
  }

  def buildRawPath(cmd: CmdConfig, dataset: Dataset, dateTokens: Array[String]): String = {
    cmd.rawPathOverride match {
      case None => cmd.folderPrefix match {
        case None => s"${dataset.hdfsPath}/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmd.reportVersion}"
        case Some(folderPrefix) => s"${dataset.hdfsPath}/$folderPrefix/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmd.reportVersion}"
      }
      case Some(rawPathOverride) => rawPathOverride
    }
  }
}
