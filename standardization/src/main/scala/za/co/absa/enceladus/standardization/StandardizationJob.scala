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

import java.io.PrintWriter
import java.io.StringWriter
import java.text.MessageFormat
import java.util.UUID

import scala.util.control.NonFatal
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits.DataSetWrapper
import za.co.absa.atum.core.{Atum, Constants}
import za.co.absa.enceladus.dao.EnceladusRestDAO
import za.co.absa.enceladus.dao.menasplugin.MenasPlugin
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.PerformanceMeasurer
import za.co.absa.enceladus.utils.performance.PerformanceMetricTools
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.enceladus.utils.validation.ValidationException

object StandardizationJob {

  private val log: Logger = LogManager.getLogger(this.getClass)
  private val conf: Config = ConfigFactory.load()

  def main(args: Array[String]) {
    implicit val spark: SparkSession = obtainSparkSession()
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

    implicit val udfLib: UDFLibrary = new UDFLibrary

    EnceladusRestDAO.login = cmd.menasCredentials
    EnceladusRestDAO.enceladusLogin()

    val dataset = EnceladusRestDAO.getDataset(cmd.datasetName, cmd.datasetVersion)
    val schema: StructType = EnceladusRestDAO.getSchema(dataset.schemaName, dataset.schemaVersion)
    val dateTokens = cmd.reportDate.split("-")

    val reportVersion = cmd.reportVersion match {
      case Some(version) => version
      case None =>
        val newVersion = fsUtils.getLatestVersion(dataset.hdfsPublishPath, cmd.reportDate) + 1
        log.warn(s"Report version not provided, inferred report version: $newVersion")
        log.warn("This is an EXPERIMENTAL feature.")
        log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
        log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
        newVersion
    }

    val path: String = buildRawPath(cmd, dataset, dateTokens, reportVersion)
    log.info(s"input path: $path")
    val stdPath = MessageFormat.format(conf.getString("standardized.hdfs.path"),
      cmd.datasetName,
      cmd.datasetVersion.toString,
      cmd.reportDate,
      reportVersion.toString)
    log.info(s"output path: $stdPath")
    // die if the output path exists
    if (fsUtils.hdfsExists(stdPath)) {
      throw new IllegalStateException(s"Path $stdPath already exists. Increment the run version, or delete $stdPath")
    }

    // init spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // init CF
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper
    spark.enableControlMeasuresTracking(s"$path/_INFO").setControlMeasuresWorkflow("Standardization")

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)
    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(cmd.datasetName, cmd.datasetVersion, isJobStageOnly = true, generateNewRun = true)

    // Add report date and version (aka Enceladus info date and version) to Atum's metadata
    Atum.setAdditionalInfo(s"enceladus_info_date" -> cmd.reportDate)
    Atum.setAdditionalInfo(s"enceladus_info_version" -> reportVersion.toString)

    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val dfAll: DataFrame = prepareDataFrame(schema, cmd, path, dataset)

    executeStandardization(performance, dfAll, schema, cmd, path, stdPath)
    cmd.performanceMetricsFile.foreach(fileName => {
      try {
        performance.writeMetricsToFile(fileName)
      } catch {
        case NonFatal(e) => log.error(s"Unable to write performance metrics to file '$fileName': ${e.getMessage}")
      }
    })
  }

  private def obtainSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Standardisation")
      .getOrCreate()
    TimeZoneNormalizer.normalizeAll(Seq(spark))
    spark
  }

  private def readerFormatSpecific(dfReader: DataFrameReader, cmd: CmdConfig, dataset: Dataset): DataFrameReader = {
    // applying format specific options
    val dfReaderWithOptions = {
      val dfr1 = if (cmd.rowTag.isDefined) dfReader.option("rowTag", cmd.rowTag.get) else dfReader
      val dfr2 = if (cmd.csvDelimiter.isDefined) dfr1.option("delimiter", cmd.csvDelimiter.get) else dfr1
      val dfr3 = if (cmd.csvHeader.isDefined) dfr2.option("header", cmd.csvHeader.get) else dfr2
      val dfr4 = if (cmd.rawFormat.equalsIgnoreCase("cobol")) applyCobolOptions(dfr3, cmd, dataset) else dfr3
      dfr4
    }
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      val dfWithFixedWithOptions = if (cmd.fixedWidthTrimValues.get) {
        dfReaderWithOptions.option("trimValues", "true")
      } else {
        dfReaderWithOptions
      }
      dfWithFixedWithOptions
    } else {
      dfReaderWithOptions
    }
  }

  private def applyCobolOptions(dfReader: DataFrameReader,
                                cmd: CmdConfig,
                                dataset: Dataset): DataFrameReader = {
    val isXcom = cmd.cobolOptions.exists(_.isXcom)
    val copybook = cmd.cobolOptions.map(_.copybook).getOrElse("")

    val reader = dfReader
      .option("is_xcom", isXcom)
      .option("schema_retention_policy", "collapse_root")

    if (copybook.isEmpty) {
      log.info("Copybook location is not provided via command line - fetching the copybook attached to the schema...")
      val copybookContents = EnceladusRestDAO.getSchemaAttachment(dataset.schemaName, dataset.schemaVersion)
      log.info(s"Applying the following copybook:\n$copybookContents")
      reader.option("copybook_contents", copybookContents)
    } else {
      log.info(s"Use copybook at $copybook")
      reader.option("copybook", copybook)
    }
  }

  private def prepareDataFrame(schema: StructType,
                               cmd: CmdConfig,
                               path: String,
                               dataset: Dataset)
                              (implicit spark: SparkSession,
                               fsUtils: FileSystemVersionUtils): DataFrame = {
    val dfReaderConfigured = readerFormatSpecific(spark.read.format(cmd.rawFormat), cmd, dataset)
    val dfWithSchema = (if (!cmd.rawFormat.equalsIgnoreCase("parquet")) {
      val inputSchema = PlainSchemaGenerator.generateInputSchema(schema).asInstanceOf[StructType]
      dfReaderConfigured.schema(inputSchema)
    } else {
      dfReaderConfigured
    }).load(s"$path/*")
    ensureSplittable(dfWithSchema, path, schema)
  }

  private def executeStandardization(performance: PerformanceMeasurer,
      dfAll: DataFrame,
      schema: StructType,
      cmd: CmdConfig,
      path: String,
      stdPath: String)(implicit spark: SparkSession, udfLib: UDFLibrary, fsUtils: FileSystemVersionUtils): Unit = {
    val rawDirSize: Long = fsUtils.getDirectorySize(path)
    performance.startMeasurement(rawDirSize)

    addRawRecordCountToMetadata(dfAll)

    val std = try {
      StandardizationInterpreter.standardize(dfAll, schema, cmd.rawFormat)
    } catch {
      case e @ ValidationException(msg, errors) =>
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

    stdRenameSourceColumns.setCheckpoint("Standardization - End", persistInDatabase = false)

    val recordCount = stdRenameSourceColumns.lastCheckpointRowCount match {
      case None    => std.count
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
    val stdDirSize = fsUtils.getDirectorySize(stdPath)
    performance.finishMeasurement(stdDirSize, recordCount)
    cmd.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    if (cmd.csvDelimiter.isDefined) {
      cmd.csvDelimiter.foreach(deLimiter => Atum.setAdditionalInfo("csv_delimiter" -> deLimiter))
    }
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "std", path, stdPath,
      EnceladusRestDAO.userName)
    stdRenameSourceColumns.writeInfoFile(stdPath)
  }

  private def ensureSplittable(df: DataFrame, path: String, schema: StructType)(implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    if (fsUtils.isNonSplittable(path)) {
      convertToSplittable(df, path, schema)
    } else {
      df
    }
  }

  private def convertToSplittable(df: DataFrame, path: String, schema: StructType)(implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    log.warn("Dataset is stored in a non-splittable format. This can have a severe performance impact.")

    val tempParquetDir = s"/tmp/nonsplittable-to-parquet-${UUID.randomUUID()}"
    log.warn(s"Converting to Parquet in temporary dir: $tempParquetDir")

    // Handle renaming of source columns in case there are columns
    // that will break because of issues in column names like spaces
    df.select(schema.fields.map { field: StructField =>
      renameSourceColumn(df, field, false)
    }: _*).write.parquet(tempParquetDir)

    fsUtils.deleteOnExit(tempParquetDir)
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

  def buildRawPath(cmd: CmdConfig, dataset: Dataset, dateTokens: Array[String], reportVersion: Int): String = {
    cmd.rawPathOverride match {
      case None =>
        val folderSuffix = s"/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v$reportVersion"
        cmd.folderPrefix match {
          case None               => s"${dataset.hdfsPath}$folderSuffix"
          case Some(folderPrefix) => s"${dataset.hdfsPath}/$folderPrefix$folderSuffix"
        }
      case Some(rawPathOverride) => rawPathOverride
    }
  }

  /**
    * Adds metadata about the number of records in raw data by checking Atum's checkpoints first.
    * If raw record count is not available in checkpoints the method will calculate that count
    * based on the provided raw dataframe.
    *
    * @return The number of records in a checkpoint corresponding to raw data (if available)
    */
  private def addRawRecordCountToMetadata(df: DataFrame): Unit = {
    val checkpointRawRecordCount = getRawRecordCountFromCheckpoints

    val rawRecordCount = checkpointRawRecordCount match {
      case Some(num) => num
      case None      => df.count
    }
    Atum.setAdditionalInfo(s"raw_record_count" -> rawRecordCount.toString)
  }

  /**
    * Gets the number of records in raw data by traversing Atum's checkpoints.
    *
    * @return The number of records in a checkpoint corresponding to raw data (if available)
    */
  private def getRawRecordCountFromCheckpoints: Option[Long] = {
    val controlMeasure = Atum.getControMeasure

    val rawCheckpoint = controlMeasure
      .checkpoints
      .find(c => c.name.equalsIgnoreCase("raw") || c.workflowName.equalsIgnoreCase("raw"))

    val measurement = rawCheckpoint.flatMap(chk => {
      chk.controls.find(m => m.controlType.equalsIgnoreCase(Constants.controlTypeRecordCount))
    })

    measurement.flatMap(m =>
      try {
        val rawCount = m.controlValue.toString.toLong
        // Basic sanity check
        if (rawCount >=0 ) {
          Some(rawCount)
        } else {
          None
        }
      }
      catch {
        case NonFatal(_) => None
      }
    )
  }

}
