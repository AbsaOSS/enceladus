/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.validation.ValidationException
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.dao.{EnceladusRestDAO, LoggedInUserInfo}
import com.typesafe.config.{Config, ConfigFactory}
import java.text.MessageFormat
import java.util.UUID

import org.apache.log4j.{LogManager, Logger}
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}

import scala.util.control.NonFatal
import za.co.absa.atum.AtumImplicits
import za.co.absa.enceladus.menasplugin.MenasPlugin
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object StandardizationJob {

  private val log: Logger = LogManager.getLogger(this.getClass)
  private val conf: Config = ConfigFactory.load()

  def main(args: Array[String]) {
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    implicit val spark: SparkSession = obtainSparkSession(cmd)
    import spark.implicits._
    implicit val udfLib: UDFLibrary = new UDFLibrary

    val menasCredentials = cmd.menasCredentials
    EnceladusRestDAO.postLogin(menasCredentials.username, menasCredentials.password)
    val dataset = EnceladusRestDAO.getDataset(cmd.datasetName, cmd.datasetVersion)
    val schema: StructType = EnceladusRestDAO.getSchema(dataset.schemaName, dataset.schemaVersion)
    val dateTokens = cmd.reportDate.split("-")
    val path: String = buildRawPath(cmd, dataset, dateTokens)
    log.info(s"input path: $path")
    val stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"),
      cmd.datasetName,
      cmd.datasetVersion.toString,
      cmd.reportDate,
      cmd.reportVersion.toString)
    log.info(s"output path: $stdPath")
    // die if the output path exists
    if (FileSystemVersionUtils.exists(stdPath)) {
      throw new IllegalStateException(s"Path $stdPath already exists. Increment the run version, or delete $stdPath")
    }
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
    val dfAll: DataFrame = prepareDataFrame(schema, cmd, path)

    executeStandardization(performance, dfAll, schema, cmd, path, stdPath)
    cmd.performanceMetricsFile.foreach(fileName => {
      try {
        performance.writeMetricsToFile(fileName)
      } catch {
        case NonFatal(e) => log.error(s"Unable to write performance metrics to file '$fileName': ${e.getMessage}")
      }
    })
  }

  private def obtainSparkSession(cmd: CmdConfig): SparkSession = {
    val spark = SparkSession.builder()
      .appName(s"Standardisation ${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} ${cmd.reportVersion}")
      .config("spark.sql.codegen.wholeStage", false) //disable whole stage code gen - the plan is too long
      .getOrCreate()
    TimeZoneNormalizer.normalizeAll(Seq(spark))
    spark
  }

  private def readerFormatSpecific(dfReader: DataFrameReader, cmd: CmdConfig):DataFrameReader = {
    // applying format specific options
    val  dfReader4= {
      val dfReader1 = if (cmd.rowTag.isDefined) dfReader.option("rowTag", cmd.rowTag.get) else dfReader
      val dfReader2 = if (cmd.csvDelimiter.isDefined) dfReader1.option("delimiter", cmd.csvDelimiter.get) else dfReader1
      val dfReader3 = if (cmd.csvHeader.isDefined) dfReader2.option("header", cmd.csvHeader.get) else dfReader2
      dfReader3
    }
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      val dfReader5=if (cmd.fixedWidthTrimValues.get) dfReader4.option("trimValues", "true") else dfReader4
      dfReader5
    } else {
      dfReader4
    }
  }

  private def prepareDataFrame(schema: StructType, cmd: CmdConfig, path: String)
                              (implicit spark: SparkSession): DataFrame = {
    val dfReaderConfigured = readerFormatSpecific(spark.read.format(cmd.rawFormat), cmd)
    val dfWithSchema = (if (!cmd.rawFormat.equalsIgnoreCase("parquet") ) {
      val inputSchema = PlainSchemaGenerator.generateInputSchema(schema).asInstanceOf[StructType]
      dfReaderConfigured.schema(inputSchema)
    } else {
      dfReaderConfigured
    }).load(s"$path/*")
    ensureSplittable(dfWithSchema, path, schema)
  }

  private def executeStandardization(performance: PerformanceMeasurer, dfAll: DataFrame, schema: StructType,
                                     cmd: CmdConfig, path: String, stdPath: String)
                                    (implicit spark: SparkSession, udfLib: UDFLibrary): Unit = {
    val rawDirSize: Long = FileSystemVersionUtils.getDirectorySize(path)
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
    val stdRenameSourceColumns: DataFrame = std.select(std.schema.fields.map { field: StructField =>
      renameSourceColumn(std, field, true)
    }: _*)

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
    // Store performance metrics
    // (record count, directory sizes, elapsed time, etc. to _INFO file metadata and performance file)
    val stdDirSize = FileSystemVersionUtils.getDirectorySize(stdPath)
    performance.finishMeasurement(stdDirSize, recordCount)
    cmd.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    if (cmd.csvDelimiter.isDefined) {
      cmd.csvDelimiter.foreach(deLimiter => Atum.setAdditionalInfo("csv_delimiter" -> deLimiter))
    }
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "std", path, stdPath,
                                                               LoggedInUserInfo.getUserName)
    stdRenameSourceColumns.writeInfoFile(stdPath)
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

    // Handle renaming of source columns in case there are columns
    // that will break because of issues in column names like spaces
    df.select(schema.fields.map { field: StructField =>
      renameSourceColumn(df, field, false)
    }: _*).write.parquet(tempParquetDir)

    FileSystemVersionUtils.deleteOnExit(tempParquetDir)
    // Reload from temp parquet and reverse column renaming above
    val dfTmp = spark.read.parquet(tempParquetDir)
    dfTmp.select(schema.fields.map { field: StructField =>
      reverseRenameSourceColumn(dfTmp, field)
    }: _*)
  }

  private def renameSourceColumn(df: DataFrame, field: StructField, registerWithATUM: Boolean): Column = {
    if (field.metadata.contains("sourcecolumn")) {
      val sourceColumnName = field.metadata.getString("sourcecolumn")
      log.info(s"schema field : ${field.name} : rename : $sourceColumnName")
      if (registerWithATUM) {
        df.registerColumnRename(sourceColumnName, field.name) //register rename with ATUM
      }
      df.col(sourceColumnName).as(field.name, field.metadata)
    } else {
      df.col(field.name)
    }
  }

  private def reverseRenameSourceColumn(df: DataFrame, field: StructField): Column = {
    if (field.metadata.contains("sourcecolumn")) {
      val sourceColumnName = field.metadata.getString("sourcecolumn")
      log.info(s"schema field : $sourceColumnName : reverse rename : ${field.name}")
      df.col(field.name).as(sourceColumnName)
    } else {
      df.col(field.name)
    }
  }

  def buildRawPath(cmd: CmdConfig, dataset: Dataset, dateTokens: Array[String]): String = {
    cmd.rawPathOverride match {
      case None =>
        val folderSuffix = s"/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmd.reportVersion}"
        cmd.folderPrefix match {
          case None => s"${dataset.hdfsPath}$folderSuffix"
          case Some(folderPrefix) => s"${dataset.hdfsPath}/$folderPrefix$folderSuffix"
        }
      case Some(rawPathOverride) => rawPathOverride
    }
  }
}
