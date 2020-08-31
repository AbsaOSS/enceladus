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
import java.util.UUID

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.atum.AtumImplicits
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.S3DefaultCredentialsProvider
import za.co.absa.enceladus.common.RecordIdGeneration.getRecordIdGenerationStrategyFromConfig
import za.co.absa.enceladus.common.config.{JobConfigParser, PathConfig}
import za.co.absa.enceladus.common.plugin.menas.MenasPlugin
import za.co.absa.enceladus.common.{CommonJobExecution, Constants}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.{StandardizationConfig, StandardizationConfigParser}
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.performance.PerformanceMetricTools
import za.co.absa.enceladus.utils.schema.{MetadataKeys, SchemaUtils, SparkUtils}
import za.co.absa.enceladus.utils.udf.UDFLibrary
import za.co.absa.enceladus.utils.validation.ValidationException

import scala.util.control.NonFatal

trait StandardizationExecution extends CommonJobExecution with S3DefaultCredentialsProvider {
  private val sourceId = SourcePhase.Standardization

  protected def prepareStandardization[T](args: Array[String],
                                          menasCredentials: MenasCredentials,
                                          preparationResult: PreparationResult)
                                         (implicit dao: MenasDAO,
                                          cmd: StandardizationConfigParser[T],
                                          fsUtils: FileSystemVersionUtils,
                                          spark: SparkSession): StructType = {

    // val stdDirSize = fsUtils.getDirectorySize(preparationResult.pathCfg.rawPath)
    // preparationResult.performance.startMeasurement(stdDirSize) // TODO fix for s3 [ref issue #1416]
    // Enable Control Framework

    // TODO fix for s3 [ref issue #1416]
    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper
//    spark.enableControlMeasuresTrackingForS3(s"${preparationResult.pathCfg.rawPath}/_INFO")
//      .setControlMeasuresWorkflow(sourceId.toString)

    log.info(s"raw path: ${preparationResult.pathCfg.rawPath}")
    log.info(s"standardization path: ${preparationResult.pathCfg.standardizationPath}")

    // Enable control framework performance optimization for pipeline-like jobs
    //Atum.setAllowUnpersistOldDatasets(true) // TODO fix for s3 [ref issue #1416]

    // Enable Menas plugin for Control Framework
    MenasPlugin.enableMenas(
      conf,
      cmd.datasetName,
      cmd.datasetVersion,
      cmd.reportDate,
      preparationResult.reportVersion)

    // Add report date and version (aka Enceladus info date and version) to Atum's metadata
    // TODO fix for s3 [ref issue #1416]
    //Atum.setAdditionalInfo(Constants.InfoDateColumn -> cmd.reportDate)
    //Atum.setAdditionalInfo(Constants.InfoVersionColumn -> preparationResult.reportVersion.toString)

    // TODO fix for s3 [ref issue #1416]
    // Add the raw format of the input file(s) to Atum's metadata
    //Atum.setAdditionalInfo("raw_format" -> cmd.rawFormat)

    // TODO fix for s3 [ref issue #1416]
//    PerformanceMetricTools.addJobInfoToAtumMetadata("std",
//      preparationResult.pathCfg.rawPath,
//      preparationResult.pathCfg.standardizationPath,
//      menasCredentials.username, args.mkString(" "))

    dao.getSchema(preparationResult.dataset.schemaName, preparationResult.dataset.schemaVersion)
  }

  override def getPathConfig[T](cmd: JobConfigParser[T], dataset: Dataset, reportVersion: Int): PathConfig = {
    val initialConfig = super.getPathConfig(cmd, dataset, reportVersion)
    cmd.asInstanceOf[StandardizationConfig].rawPathOverride match {
      case None => initialConfig
      case Some(providedRawPath) => initialConfig.copy(rawPath = providedRawPath)
    }
  }

  override def validateOutputPath(fsUtils: FileSystemVersionUtils, pathConfig: PathConfig): Unit = {
    validateIfPathAlreadyExists(fsUtils: FileSystemVersionUtils, pathConfig.standardizationPath)
  }

  protected def readStandardizationInputData[T](schema: StructType,
                                                cmd: StandardizationConfigParser[T],
                                                path: String,
                                                dataset: Dataset)
                                               (implicit spark: SparkSession,
                                                fsUtils: FileSystemVersionUtils,
                                                dao: MenasDAO): DataFrame = {
    val numberOfColumns = schema.fields.length
    val standardizationReader = new StandardizationPropertiesProvider()
    val dfReaderConfigured = standardizationReader.getFormatSpecificReader(cmd, dataset, numberOfColumns)
    val readerWithOptSchema = cmd.rawFormat.toLowerCase() match {
      case "parquet" | "cobol" => dfReaderConfigured
      case _ =>
        val optColumnNameOfCorruptRecord = getColumnNameOfCorruptRecord(schema, cmd)
        val inputSchema = PlainSchemaGenerator.generateInputSchema(schema, optColumnNameOfCorruptRecord)
        dfReaderConfigured.schema(inputSchema)
    }
    val dfWithSchema = readerWithOptSchema.load(s"$path/*")

    ensureSplittable(dfWithSchema, path, schema)
  }

  private def getColumnNameOfCorruptRecord[R](schema: StructType, cmd: StandardizationConfigParser[R])
                                             (implicit spark: SparkSession): Option[String] = {
    // SparkUtils.setUniqueColumnNameOfCorruptRecord is called even if result is not used to avoid conflict
    val columnNameOfCorruptRecord = SparkUtils.setUniqueColumnNameOfCorruptRecord(spark, schema)
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width") || cmd.failOnInputNotPerSchema) {
      None
    } else {
      Option(columnNameOfCorruptRecord)
    }
  }

  protected def standardize[T](inputData: DataFrame, schema: StructType, cmd: StandardizationConfigParser[T])
                              (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame = {
    //scalastyle:on parameter.number
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    try {
      //handleControlInfoValidation() // TODO fix for s3 [ref issue #1416]
      StandardizationInterpreter.standardize(inputData, schema, cmd.rawFormat,
        cmd.failOnInputNotPerSchema, recordIdGenerationStrategy)
    } catch {
      case e@ValidationException(msg, errors) =>
        val errorDescription = s"$msg\nDetails: ${errors.mkString("\n")}"
        // TODO fix for s3 [ref issue #1416]
        //AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Schema Validation", errorDescription, "")
        throw e
      case NonFatal(e) if !e.isInstanceOf[ValidationException] =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        // TODO fix for s3 [ref issue #1416]
        //AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(sourceId.toString, e.getMessage, sw.toString)
        throw e
    }
  }

  protected def processStandardizationResult[T](args: Array[String],
                                                standardizedDF: DataFrame,
                                                preparationResult: PreparationResult,
                                                schema: StructType,
                                                cmd: StandardizationConfigParser[T],
                                                menasCredentials: MenasCredentials)
                                               (implicit spark: SparkSession,
                                                fsUtils: FileSystemVersionUtils): DataFrame = {
    import za.co.absa.atum.AtumImplicits._
    val fieldRenames = SchemaUtils.getRenamesInSchema(schema)
    fieldRenames.foreach {
      case (destinationName, sourceName) => standardizedDF.registerColumnRename(sourceName, destinationName)
    }

    // standardizedDF.setCheckpoint(s"$sourceId - End", persistInDatabase = false)  // TODO fix for s3 [ref issue #1416]

    // TODO fix for s3 [ref issue #1416]
//    val recordCount = standardizedDF.lastCheckpointRowCount match {
//      case None => standardizedDF.count
//      case Some(p) => p
//    }
    val recordCount = standardizedDF.count()

    if (recordCount == 0) {
      handleEmptyOutput(sourceId)
    }

    log.info(s"Writing into standardized path ${preparationResult.pathCfg.standardizationPath}")
    standardizedDF.write.parquet(preparationResult.pathCfg.standardizationPath)
    // Store performance metrics
    // (record count, directory sizes, elapsed time, etc. to _INFO file metadata and performance file)

    // TODO fix for s3 [ref issue #1416]
    // val stdDirSize = fsUtils.getDirectorySize(preparationResult.pathCfg.standardizationPath)
    // preparationResult.performance.finishMeasurement(stdDirSize, recordCount)
//    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(
//      spark,
//      "std",
//      preparationResult.pathCfg.rawPath,
//      preparationResult.pathCfg.standardizationPath,
//      menasCredentials.username,
//      args.mkString(" ")
//    )

    // TODO fix for s3 [ref issue #1416]
    //cmd.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    //cmd.csvDelimiter.foreach(delimiter => Atum.setAdditionalInfo("csv_delimiter" -> delimiter))

    // standardizedDF.writeInfoFile(preparationResult.pathCfg.standardizationPath)  // TODO fix for s3 [ref issue #1416]
    //writePerformanceMetrics(preparationResult.performance, cmd)
    log.info(s"$sourceId finished successfully")
    standardizedDF
  }

  //scalastyle:off parameter.number

  private def ensureSplittable(df: DataFrame, path: String, schema: StructType)
                              (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    // TODO fix for s3 [ref issue #1416]
//    if (fsUtils.isNonSplittable(path)) {
//      convertToSplittable(df, schema)
//    } else {
      df
//    }
  }

  private def convertToSplittable(df: DataFrame, schema: StructType)
                                 (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    log.warn("Dataset is stored in a non-splittable format. This can have a severe performance impact.")

    val tempParquetDir = s"/tmp/nonsplittable-to-parquet-${UUID.randomUUID()}"
    log.warn(s"Converting to Parquet in temporary dir: $tempParquetDir")

    // Handle renaming of source columns in case there are columns
    // that will break because of issues in column names like spaces
    df.select(schema.fields.map { field: StructField =>
      renameSourceColumn(df, field)
    }: _*).write.parquet(tempParquetDir)

    fsUtils.deleteOnExit(tempParquetDir)
    // Reload from temp parquet and reverse column renaming above
    val dfTmp = spark.read.parquet(tempParquetDir)
    dfTmp.select(schema.fields.map { field: StructField =>
      reverseRenameSourceColumn(dfTmp, field)
    }: _*)
  }

  private def renameSourceColumn(df: DataFrame, field: StructField): Column = {
    if (field.metadata.contains(MetadataKeys.SourceColumn)) {
      val sourceColumnName = field.metadata.getString(MetadataKeys.SourceColumn)
      log.info(s"schema field : ${field.name} : rename : $sourceColumnName")
      df.col(sourceColumnName).as(field.name, field.metadata)
    } else {
      df.col(field.name)
    }
  }

  private def reverseRenameSourceColumn(df: DataFrame, field: StructField): Column = {
    if (field.metadata.contains(MetadataKeys.SourceColumn)) {
      val sourceColumnName = field.metadata.getString(MetadataKeys.SourceColumn)
      log.info(s"schema field : $sourceColumnName : reverse rename : ${field.name}")
      df.col(field.name).as(sourceColumnName)
    } else {
      df.col(field.name)
    }
  }

}
