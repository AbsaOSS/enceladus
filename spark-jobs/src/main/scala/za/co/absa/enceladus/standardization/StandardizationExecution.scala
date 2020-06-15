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
import za.co.absa.enceladus.common.RecordIdGeneration.getRecordIdGenerationStrategyFromConfig
import za.co.absa.enceladus.common.{CommonJobExecution, PathCfg}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.{PerformanceMeasurer, PerformanceMetricTools}
import za.co.absa.enceladus.utils.schema.{MetadataKeys, SchemaUtils, SparkUtils}
import za.co.absa.enceladus.utils.udf.UDFLibrary
import za.co.absa.enceladus.utils.validation.ValidationException

import scala.util.control.NonFatal

trait StandardizationExecution extends CommonJobExecution {
  protected implicit val step = "Standardization"

  protected def getPathCfg(cmd: StandardizationCmdConfig, dataset: Dataset, reportVersion: Int): PathCfg = {
    PathCfg(
      inputPath = buildRawPath(cmd, dataset, reportVersion),
      outputPath = getStandardizationPath(cmd.jobConfig, reportVersion)
    )
  }

  def buildRawPath(cmd: StandardizationCmdConfig, dataset: Dataset, reportVersion: Int): String = {
    val dateTokens = cmd.jobConfig.reportDate.split("-")
    cmd.stdConfig.rawPathOverride match {
      case None =>
        val folderSuffix = s"/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v$reportVersion"
        cmd.jobConfig.folderPrefix match {
          case None => s"${dataset.hdfsPath}$folderSuffix"
          case Some(folderPrefix) => s"${dataset.hdfsPath}/$folderPrefix$folderSuffix"
        }
      case Some(rawPathOverride) => rawPathOverride
    }
  }

  protected def prepareDataFrame(schema: StructType,
                                 cmd: StandardizationCmdConfig,
                                 path: String,
                                 dataset: Dataset)
                                (implicit spark: SparkSession,
                                 fsUtils: FileSystemVersionUtils,
                                 dao: MenasDAO): DataFrame = {
    val numberOfColumns = schema.fields.length
    val standardizationReader = new StandardizationReader(log)
    val dfReaderConfigured = standardizationReader.getFormatSpecificReader(cmd, dataset, numberOfColumns)
    val dfWithSchema = (if (!cmd.stdConfig.rawFormat.equalsIgnoreCase("parquet")) {
      // SparkUtils.setUniqueColumnNameOfCorruptRecord is called even if result is not used to avoid conflict
      val columnNameOfCorruptRecord = SparkUtils.setUniqueColumnNameOfCorruptRecord(spark, schema)
      val optColumnNameOfCorruptRecord = if (cmd.stdConfig.failOnInputNotPerSchema) {
        None
      } else {
        Option(columnNameOfCorruptRecord)
      }
      val inputSchema = PlainSchemaGenerator.generateInputSchema(schema, optColumnNameOfCorruptRecord)
      dfReaderConfigured.schema(inputSchema)
    } else {
      dfReaderConfigured
    }).load(s"$path/*")
    ensureSplittable(dfWithSchema, path, schema)
  }

  private def ensureSplittable(df: DataFrame, path: String, schema: StructType)
                              (implicit spark: SparkSession, fsUtils: FileSystemVersionUtils) = {
    if (fsUtils.isNonSplittable(path)) {
      convertToSplittable(df, schema)
    } else {
      df
    }
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

  protected def standardize(dfAll: DataFrame, schema: StructType, cmd: StandardizationCmdConfig)
                           (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame = {
    //scalastyle:on parameter.number
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    try {
      handleControlInfoValidation()
      StandardizationInterpreter.standardize(dfAll, schema, cmd.stdConfig.rawFormat,
        cmd.stdConfig.failOnInputNotPerSchema, recordIdGenerationStrategy)
    } catch {
      case e@ValidationException(msg, errors) =>
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError("Schema Validation", s"$msg\nDetails: ${
          errors.mkString("\n")
        }", "")
        throw e
      case NonFatal(e) if !e.isInstanceOf[ValidationException] =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        AtumImplicits.SparkSessionWrapper(spark).setControlMeasurementError(step, e.getMessage, sw.toString)
        throw e
    }
  }

  protected def processStandardizationResult(standardizedDF: DataFrame,
                                             performance: PerformanceMeasurer,
                                             pathCfg: PathCfg,
                                             schema: StructType, cmd: StandardizationCmdConfig,
                                             menasCredentials: MenasCredentials)
                                            (implicit spark: SparkSession,
                                             fsUtils: FileSystemVersionUtils): Unit = {
    //register renames with ATUM
    import za.co.absa.atum.AtumImplicits._
    val fieldRenames = SchemaUtils.getRenamesInSchema(schema)
    fieldRenames.foreach {
      case (destinationName, sourceName) => standardizedDF.registerColumnRename(sourceName, destinationName)
    }

    standardizedDF.setCheckpoint(s"$step - End", persistInDatabase = false)

    val recordCount = standardizedDF.lastCheckpointRowCount match {
      case None => standardizedDF.count
      case Some(p) => p
    }
    if (recordCount == 0) {
      handleEmptyOutputAfterStep()
    }

    standardizedDF.write.parquet(pathCfg.outputPath)
    // Store performance metrics
    // (record count, directory sizes, elapsed time, etc. to _INFO file metadata and performance file)
    val stdDirSize = fsUtils.getDirectorySize(pathCfg.outputPath)
    performance.finishMeasurement(stdDirSize, recordCount)
    PerformanceMetricTools.addPerformanceMetricsToAtumMetadata(spark, "std", pathCfg.inputPath, pathCfg.outputPath,
      menasCredentials.username, cmd.jobConfig.args.mkString(" "))

    cmd.stdConfig.rowTag.foreach(rowTag => Atum.setAdditionalInfo("xml_row_tag" -> rowTag))
    if (cmd.stdConfig.csvDelimiter.isDefined) {
      cmd.stdConfig.csvDelimiter.foreach(delimiter => Atum.setAdditionalInfo("csv_delimiter" -> delimiter))
    }

    standardizedDF.writeInfoFile(pathCfg.outputPath)
    writePerformanceMetrics(performance, cmd.jobConfig)
  }
}
