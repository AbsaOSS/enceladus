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
import za.co.absa.enceladus.standardization.interpreter.dataTypes._
import za.co.absa.enceladus.standardization.interpreter.stages.{SchemaChecker, SparkXMLHack, TypeParser}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary, UDFunctionNames}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.validation.ValidationException

/**
  * Object representing set of tools for performing the actual standardization
  */
object StandardizationInterpreter{
  private implicit val defaults: Defaults = GlobalDefaults
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Perform the standardization of the dataframe given the expected schema
    * @param df         Dataframe to be standardized
    * @param expSchema  The schema for the df to be standardized into
    */
  def standardize(df: Dataset[Row], expSchema: StructType, inputType: String, failFast: Boolean = false)
                 (implicit spark: SparkSession, udfLib: UDFLibrary): Dataset[Row] = {

    logger.info(s"Step 1: Schema validation")
    validateSchemaAgainstSelfInconsistencies(expSchema)

    // TODO: remove when spark-xml handles empty arrays #417
    val dfXmlSafe: Dataset[Row] = if (inputType.toLowerCase() == "xml") {
      df.select(expSchema.fields.map { field: StructField =>
        SparkXMLHack.hack(field, "", df).as(field.name)
      }: _*)
    } else {
      df
    }

    logger.info(s"Step 2: Standardization")
    val std = standardizeDataset(dfXmlSafe, expSchema, failFast)

    logger.info(s"Step 3: Clean the final error column")
    val cleanedStd = cleanTheFinalErrorColumn(std)
    logger.info(s"Standardization process finished, returning to the application...")
    cleanedStd
  }

  private def validateSchemaAgainstSelfInconsistencies(expSchema: StructType)
                                                      (implicit spark: SparkSession): Unit = {
    val validationErrors = SchemaChecker.validateSchemaAndLog(expSchema)
    if (validationErrors._1.nonEmpty) {
      throw new ValidationException("A fatal schema validation error occurred.", validationErrors._1)
    }
  }

  private def standardizeDataset(df: Dataset[Row], expSchema: StructType, failFast: Boolean)
                                (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame  = {

    val rowErrors: List[Column] = gatherRowErrors(df.schema)
    val (stdCols, errorCols, oldErrorColumn) = expSchema.fields.foldLeft(List.empty[Column], rowErrors, None: Option[Column]) {
      (acc, field) =>
        logger.info(s"Standardizing field: ${field.name}")
        val (accCols, accErrorCols, accOldErrorColumn) = acc
        if (field.name == ErrorMessage.errorColumnName) {
          (accCols, accErrorCols, Option(df.col(field.name)))
        } else {
          val ParseOutput(stdColumn, errColumn) = TypeParser.standardize(field, "", df.schema, failFast)
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
      .withColumn(ErrorMessage.errorColumnName, callUDF(UDFunctionNames.cleanErrCol, col(ErrorMessage.errorColumnName)))
  }

  private def gatherRowErrors(origSchema: StructType)(implicit spark: SparkSession): List[Column] = {
    val corruptRecordColumn = spark.conf.get("spark.sql.columnNameOfCorruptRecord")
    SchemaUtils.getField(corruptRecordColumn, origSchema).map {_ =>
      val column = col(corruptRecordColumn)
      when(column.isNotNull, // input row was not per expected schema
        array(callUDF(UDFunctionNames.stdSchemaErr, column.cast(StringType)) //column should be StringType but better to be sure
      ).otherwise( // schema is OK
        typedLit(Seq.empty[ErrorMessage])
      ))
    }.toList
  }
}
