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
import java.text.MessageFormat
import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.AtumImplicits.DataSetWrapper
import za.co.absa.atum.core.{Atum, Constants}
import za.co.absa.enceladus.dao.{MenasDAO, RestDaoFactory}
import za.co.absa.enceladus.dao.menasplugin.MenasPlugin
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.enceladus.utils.validation.ValidationException

import scala.collection.immutable.HashMap
import scala.util.Try
import scala.util.control.NonFatal

object StandardizationJob {
  TimeZoneNormalizer.normalizeJVMTimeZone()

  private val log = LoggerFactory.getLogger(this.getClass)
  private val conf = ConfigFactory.load()
  private final val SparkCSVReaderMaxColumnsDefault: Int = 20480

  def main(args: Array[String]) {
    implicit val spark: SparkSession = obtainSparkSession()
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

    implicit val udfLib: UDFLibrary = new UDFLibrary
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(cmd.menasCredentials)

    dao.authenticate()

    val dataset = dao.getDataset(cmd.datasetName, cmd.datasetVersion)
    val schema: StructType = dao.getSchema(dataset.schemaName, dataset.schemaVersion)
    val dateTokens = cmd.reportDate.split("-")

    val reportVersion = cmd.reportVersion match {
      case Some(version) => version
      case None          =>
        val newVersion = fsUtils.getLatestVersion(dataset.hdfsPublishPath, cmd.reportDate) + 1
        log.warn(s"Report version not provided, inferred report version: $newVersion")
        log.warn("This is an EXPERIMENTAL feature.")
        log.warn(" -> It can lead to issues when running multiple jobs on a dataset concurrently.")
        log.warn(" -> It may not work as desired when there are gaps in the versions of the data being landed.")
        newVersion
    }

    val pathCfg = PathCfg(
      inputPath = buildRawPath(cmd, dataset, dateTokens, reportVersion),
      outputPath = MessageFormat.format(conf.getString("standardized.hdfs.path"),
        cmd.datasetName,
        cmd.datasetVersion.toString,
        cmd.reportDate,
        reportVersion.toString)
    )
    log.info(s"input path: ${pathCfg.inputPath}")
    log.info(s"output path: ${pathCfg.outputPath}")
    // die if the output path exists
    if (fsUtils.hdfsExists(pathCfg.outputPath)) {
      throw new IllegalStateException(s"Path ${pathCfg.outputPath} already exists. Increment the run version, or delete ${pathCfg.outputPath}")
    }

    // init spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    // init CF
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper
    spark.enableControlMeasuresTracking(s"${pathCfg.inputPath}/_INFO").setControlMeasuresWorkflow("Standardization")

    // Enable control framework performance optimization for pipeline-like jobs
    Atum.setAllowUnpersistOldDatasets(true)
    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(cmd.datasetName, cmd.datasetVersion, isJobStageOnly = true, generateNewRun = true)

    // Add report date and version (aka Enceladus info date and version) to Atum's metadata
    Atum.setAdditionalInfo("enceladus_info_date" -> cmd.reportDate)
    Atum.setAdditionalInfo("enceladus_info_version" -> reportVersion.toString)

    // Add the raw format of the input file(s) to Atum's metadta as well
    Atum.setAdditionalInfo("raw_format" -> cmd.rawFormat)

    // init performance measurer
    val performance = new PerformanceMeasurer(spark.sparkContext.appName)
    val dfAll: DataFrame = prepareDataFrame(schema, cmd, pathCfg.inputPath, dataset)

    executeStandardization(performance, dfAll, schema, cmd, pathCfg)
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
    TimeZoneNormalizer.normalizeSessionTimeZone(spark)
    spark
  }

  /**
    * Returns a Spark reader with all format-specific options applied.
    * Options are provided by command line parameters.
    *
    * @param cmd      Command line parameters containing format-specific options
    * @param dataset  A dataset definition
    * @param numberOfColumns (Optional) number of columns, enables reading CSV files with the number of columns
    *                        larger than Spark default
    * @return The updated dataframe reader
    */
  def getFormatSpecificReader(cmd: CmdConfig, dataset: Dataset, numberOfColumns: Int = 0)
                             (implicit spark: SparkSession, dao: MenasDAO): DataFrameReader = {
    val dfReader = spark.read.format(cmd.rawFormat)
    // applying format specific options
    val options = getCobolOptions(cmd, dataset) ++
      getGenericOptions(cmd) ++
      getXmlOptions(cmd) ++
      getCsvOptions(cmd, numberOfColumns) ++
      getFixedWidthOptions(cmd)

    // Applying all the options
    options.foldLeft(dfReader) { (df, optionPair) =>
      optionPair match {
        case (key, Some(value)) =>
          value match {
            // Handle all .option() overloads
            case StringParameter(s) => df.option(key, s)
            case BooleanParameter(b) => df.option(key, b)
            case LongParameter(l) => df.option(key, l)
            case DoubleParameter(d) => df.option(key, d)
          }
        case (_, None)          => df
      }
    }
  }

  private def getGenericOptions(cmd: CmdConfig): HashMap[String,Option[RawFormatParameter]] = {
    HashMap("charset" -> cmd.charset.map(StringParameter))
  }

  private def getXmlOptions(cmd: CmdConfig): HashMap[String,Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("xml")) {
      HashMap("rowtag" -> cmd.rowTag.map(StringParameter))
    } else {
      HashMap()
    }
  }

  private def getCsvOptions(cmd: CmdConfig, numberOfColumns: Int = 0): HashMap[String,Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("csv")) {
      HashMap(
        "delimiter" -> cmd.csvDelimiter.map(StringParameter),
        "header" -> cmd.csvHeader.map(BooleanParameter),
        "quote" -> cmd.csvQuote.map(StringParameter),
        "escape" -> cmd.csvEscape.map(StringParameter),
        // increase the default limit on the number of columns if needed
        // default is set at org.apache.spark.sql.execution.datasources.csv.CSVOptions maxColumns
        "maxColumns" -> {if (numberOfColumns > SparkCSVReaderMaxColumnsDefault) Some(LongParameter(numberOfColumns)) else None}
      )
    } else {
      HashMap()
    }
  }

  private def getFixedWidthOptions(cmd: CmdConfig): HashMap[String,Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      HashMap("trimValues" -> cmd.fixedWidthTrimValues.map(BooleanParameter))
    } else {
      HashMap()
    }
  }

  private def getCobolOptions(cmd: CmdConfig, dataset: Dataset)(implicit dao: MenasDAO): HashMap[String, Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("cobol")) {
      val cobolOptions = cmd.cobolOptions.getOrElse(CobolOptions())
      HashMap(
        getCopybookOption(cobolOptions, dataset),
        "is_xcom" -> Option(BooleanParameter(cobolOptions.isXcom)),
        "schema_retention_policy" -> Some(StringParameter("collapse_root"))
      )
    } else {
      HashMap()
    }
  }

  private def getCopybookOption(opts: CobolOptions, dataset: Dataset)(implicit dao: MenasDAO): (String, Option[RawFormatParameter]) = {
    val copybook = opts.copybook
    if (copybook.isEmpty) {
      log.info("Copybook location is not provided via command line - fetching the copybook attached to the schema...")
      val copybookContents = dao.getSchemaAttachment(dataset.schemaName, dataset.schemaVersion)
      log.info(s"Applying the following copybook:\n$copybookContents")
      ("copybook_contents", Option(StringParameter(copybookContents)))
    } else {
      log.info(s"Use copybook at $copybook")
      ("copybook", Option(StringParameter(copybook)))
    }
  }

  private def prepareDataFrame(schema: StructType,
                               cmd: CmdConfig,
                               path: String,
                               dataset: Dataset)
                              (implicit spark: SparkSession,
                               fsUtils: FileSystemVersionUtils,
                               dao: MenasDAO): DataFrame = {
    val numberOfColumns = schema.fields.length
    val dfReaderConfigured = getFormatSpecificReader(cmd, dataset, numberOfColumns)
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
                                     pathCfg: PathCfg)
                                    (implicit spark: SparkSession, udfLib: UDFLibrary, fsUtils: FileSystemVersionUtils): Unit = {
    val rawDirSize: Long = fsUtils.getDirectorySize(pathCfg.inputPath)
    performance.startMeasurement(rawDirSize)

    addRawRecordCountToMetadata(dfAll)

    val std = try {
      StandardizationInterpreter.standardize(dfAll, schema, cmd.rawFormat)
    } catch {
      case e@ValidationException(msg, errors)                  =>
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
      renameSourceColumn(std, field, registerWithATUM = true)
    }: _*)

    stdRenameSourceColumns.setCheckpoint("Standardization - End", persistInDatabase = false)

    val recordCount = stdRenameSourceColumns.lastCheckpointRowCount match {
      case None    => std.count
      case Some(p) => p
    }
    if (recordCount == 0) { handleEmptyOutputAfterStandardization() }

    stdRenameSourceColumns.write.parquet(pathCfg.outputPath)
    // Store performance metrics
    // (record count, directory sizes, elapsed time, etc. to _INFO file metadata and performance file)
    val stdDirSize = fsUtils.getDirectorySize(pathCfg.outputPath)
    performance.finishMeasurement(stdDirSize, recordCount)
    cmd.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    if (cmd.csvDelimiter.isDefined) {
      cmd.csvDelimiter.foreach(delimiter => Atum.setAdditionalInfo("csv_delimiter" -> delimiter))
    }
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "std", pathCfg.inputPath, pathCfg.outputPath,
      cmd.menasCredentials.get.username, cmd.cmdLineArgs.mkString(" "))
    stdRenameSourceColumns.writeInfoFile(pathCfg.outputPath)
  }

  private def handleEmptyOutputAfterStandardization()(implicit spark: SparkSession): Unit = {
    import za.co.absa.atum.core.Constants._

    val areCountMeasurementsAllZero = Atum.getControMeasure.checkpoints
      .flatMap(checkpoint =>
        checkpoint.controls.filter(control =>
          control.controlName.equalsIgnoreCase(controlTypeRecordCount)))
      .forall(m => Try(m.controlValue.toString.toDouble).toOption.contains(0D))

    if (areCountMeasurementsAllZero) {
      log.warn("Empty output after running Standardization. Previous checkpoints show this is correct.")
    } else {
      val errMsg = "Empty output after running Standardization, while previous checkpoints show non zero record count"
      AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Standardization", errMsg, "")
      throw new IllegalStateException(errMsg)
    }
  }

  private def ensureSplittable(df: DataFrame, path: String, schema: StructType)
                              (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    if (fsUtils.isNonSplittable(path)) {
      convertToSplittable(df, path, schema)
    } else {
      df
    }
  }

  private def convertToSplittable(df: DataFrame, path: String, schema: StructType)
                                 (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    log.warn("Dataset is stored in a non-splittable format. This can have a severe performance impact.")

    val tempParquetDir = s"/tmp/nonsplittable-to-parquet-${UUID.randomUUID()}"
    log.warn(s"Converting to Parquet in temporary dir: $tempParquetDir")

    // Handle renaming of source columns in case there are columns
    // that will break because of issues in column names like spaces
    df.select(schema.fields.map { field: StructField =>
      renameSourceColumn(df, field, registerWithATUM = false)
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
      case None                  =>
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
        if (rawCount >= 0) {
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

  private final case class PathCfg(inputPath: String, outputPath: String)
}
