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

package za.co.absa.enceladus.standardization.interpreter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.common.{Constants, RecordIdGeneration}
import za.co.absa.enceladus.common.RecordIdGeneration._
import za.co.absa.enceladus.standardization.interpreter.dataTypes._
import za.co.absa.enceladus.standardization.interpreter.stages.{SchemaChecker, SparkXMLHack, TypeParser}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.{SchemaUtils, SparkUtils}
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.utils.udf.{UDFLibrary, UDFNames}
import za.co.absa.enceladus.utils.validation.ValidationException

/**
 * Object representing set of tools for performing the actual standardization
 */
object StandardizationInterpreter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Perform the standardization of the dataframe given the expected schema
   *
   * @param df                         Dataframe to be standardized
   * @param expSchema                  The schema for the df to be standardized into
   * @param failOnInputNotPerSchema    if true a discrepancy between expSchema and input data throws an exception
   *                                   if false the error is marked in the error column
   * @param recordIdGenerationStrategy Decides if true uuid, pseudo (always the same) is used for the
   *                                   [[Constants.EnceladusRecordId]] or if the column is not added at all [[IdType.NoId]] (default).
   */
  def standardize(df: Dataset[Row], expSchema: StructType, inputType: String, failOnInputNotPerSchema: Boolean = false,
                  recordIdGenerationStrategy: IdType = IdType.NoId)
                 (implicit spark: SparkSession, udfLib: UDFLibrary, defaults: Defaults): Dataset[Row] = {

    logger.info(s"Step 1: Schema validation")
    validateSchemaAgainstSelfInconsistencies(expSchema)

    // TODO: remove when spark-xml handles empty arrays #417
    val dfXmlSafe: Dataset[Row] = if (inputType == "xml") {
      df.select(expSchema.fields.map { field: StructField =>
        SparkXMLHack.hack(field, "", df).as(field.name)
      }: _*)
    } else {
      df
    }

    logger.info(s"Step 2: Standardization")
    val std = standardizeDataset(dfXmlSafe, expSchema, failOnInputNotPerSchema)

    logger.info(s"Step 3: Clean the final error column")
    val cleanedStd = cleanTheFinalErrorColumn(std)

    val idedStd = if (SchemaUtils.fieldExists(Constants.EnceladusRecordId, cleanedStd.schema)) {
      cleanedStd // no new id regeneration
    } else {
      RecordIdGeneration.addRecordIdColumnByStrategy(cleanedStd, Constants.EnceladusRecordId, recordIdGenerationStrategy)
    }

    logger.info(s"Standardization process finished, returning to the application...")
    idedStd
  }

  private def validateSchemaAgainstSelfInconsistencies(expSchema: StructType)
                                                      (implicit spark: SparkSession): Unit = {
    val validationErrors = SchemaChecker.validateSchemaAndLog(expSchema)
    if (validationErrors._1.nonEmpty) {
      throw new ValidationException("A fatal schema validation error occurred.", validationErrors._1)
    }
  }

  private def standardizeDataset(df: Dataset[Row], expSchema: StructType, failOnInputNotPerSchema: Boolean)
                                (implicit spark: SparkSession, udfLib: UDFLibrary, defaults: Defaults): DataFrame  = {

    val rowErrors: List[Column] = gatherRowErrors(df.schema)
    val (stdCols, errorCols, oldErrorColumn) = expSchema.fields.foldLeft(List.empty[Column], rowErrors, None: Option[Column]) {
      (acc, field) =>
        logger.info(s"Standardizing field: ${field.name}")
        val (accCols, accErrorCols, accOldErrorColumn) = acc
        if (field.name == ErrorMessage.errorColumnName) {
          (accCols, accErrorCols, Option(df.col(field.name)))
        } else {
          val ParseOutput(stdColumn, errColumn) = TypeParser.standardize(field, "", df.schema, failOnInputNotPerSchema)
          logger.info(s"Applying standardization plan for ${field.name}")
          (stdColumn :: accCols, errColumn :: accErrorCols, accOldErrorColumn)
        }
    }

    val errorColsAllInCorrectOrder: List[Column] = (oldErrorColumn.toList ++ errorCols).reverse
    val cols = (array(errorColsAllInCorrectOrder: _*) as ErrorMessage.errorColumnName) :: stdCols
    df.select(cols.reverse: _*)
  }

  private def cleanTheFinalErrorColumn(dataFrame: DataFrame)
                                      (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame = {
    ArrayTransformations.flattenArrays(dataFrame, ErrorMessage.errorColumnName)
      .withColumn(ErrorMessage.errorColumnName, callUDF(UDFNames.cleanErrCol, col(ErrorMessage.errorColumnName)))
  }

  private def gatherRowErrors(origSchema: StructType)(implicit spark: SparkSession): List[Column] = {
    val corruptRecordColumn = spark.conf.get(SparkUtils.ColumnNameOfCorruptRecordConf)
    SchemaUtils.getField(corruptRecordColumn, origSchema).map {_ =>
      val column = col(corruptRecordColumn)
      when(column.isNotNull, // input row was not per expected schema
        array(callUDF(UDFNames.stdSchemaErr, column.cast(StringType)) //column should be StringType but better to be sure
      )).otherwise( // schema is OK
        typedLit(Seq.empty[ErrorMessage])
      )
    }.toList
  }
}
