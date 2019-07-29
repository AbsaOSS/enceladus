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

package za.co.absa.enceladus.standardization.interpreter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import za.co.absa.enceladus.standardization.StandardizationCommon
import za.co.absa.enceladus.standardization.interpreter.dataTypes._
import za.co.absa.enceladus.standardization.interpreter.stages.{SchemaChecker, SparkXMLHack, TypeParser}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.validation.ValidationException

/**
  * Object representing set of tools for performing the actual standardization
  */
object StandardizationInterpreter extends StandardizationCommon{

  private type ErrorCols = List[String]

  /**
    * Perform the standardization of the dataframe given the expected schema
    * @param df Dataframe to be standardized
    * @param expSchema The schema for the df to be standardized into
    */
  def standardize(df: Dataset[Row], expSchema: StructType, inputType: String)
                 (implicit spark: SparkSession, udfLib: UDFLibrary): Dataset[Row] = {

    logger.info(s"Step 1: Schema validation")
    validateSchemaAgainstSelfInconsistencies(expSchema)

    // TODO: remove when spark-xml handles empty arrays #417
    val df1: Dataset[Row] = if (inputType.toLowerCase() == "xml") {
      df.select(expSchema.fields.map { field: StructField =>
        SparkXMLHack.hack(field, "", df).as(field.name)
      }: _*)
    } else {
      df
    }

    logger.info(s"Step 2: Standardization")
    val (std, errorColsAfterStandardization) = standardizeDataset(df1, expSchema)

    logger.info(s"Step 3.1: Preserve existing error column")
    val (stdWithErrColPreserved, errorColsWithErrColPreserved) = preserveExistingErrorColumn(
      std,
      errorColsAfterStandardization
    )
    logger.info(s"Step 3.2: Collect all temporary error columns")
    val stdWithCollectedTemporaryColumns =
      collectAllTempErrorColumns(stdWithErrColPreserved, errorColsWithErrColPreserved)
    logger.info(s"Step 3.3: Drop all temporary error columns")
    val stdWithTempErrorColumnsDropped =
      dropAllTemporaryErrorColumn(stdWithCollectedTemporaryColumns, errorColsWithErrColPreserved)
    logger.info(s"Step 3.4: Clean the final error column")
    val cleanedStd = cleanTheFinalErrorColumn(stdWithTempErrorColumnsDropped, errorColsWithErrColPreserved)
    logger.info(s"Standardization process finished, returning to the application...")
    cleanedStd
  }

  private def stdApplyLogic(stdOutput: ParseOutput, field: StructField, nextErrorColumnIndex: Int)
                           (implicit spark: SparkSession, udfLib: UDFLibrary): (Column, Column, String) = {
    val ParseOutput(stdCol, errs) = stdOutput
    val errField = s"error_${unpath(field.name)}_$nextErrorColumnIndex"
    // If the meta data value sourcecolumn is set override the field name
    val fieldName = SchemaUtils.getFieldNameOverriddenByMetadata(field)
    (errs.as(errField),stdCol as fieldName, errField)
  }


  private def validateSchemaAgainstSelfInconsistencies(expSchema: StructType)
                                                      (implicit spark: SparkSession): Unit = {
    val validationErrors = SchemaChecker.validateSchemaAndLog(expSchema)
    if (validationErrors._1.nonEmpty) {
      throw new ValidationException("A fatal schema validation error occurred.", validationErrors._1)
    }
  }

  private def standardizeDataset(df: Dataset[Row], expSchema: StructType)
                                (implicit spark: SparkSession, udfLib: UDFLibrary): (DataFrame, ErrorCols)  = {

    val (cols, errorColNames, _) = expSchema.fields.foldLeft(List.empty[Column], List.empty[String], 1) {
      (acc, field) =>
        logger.info(s"Standardizing field: ${field.name}")
        val (accCols, accErrorCols, nextErrorColumnIndex) = acc
        if (field.name == ErrorMessage.errorColumnName) {
          (df.col(field.name) :: accCols, accErrorCols, nextErrorColumnIndex)
        } else {
          val stdOutput = TypeParser.standardize(field, "", df.schema)
          logger.info(s"Applying standardization plan for ${field.name}")
          val (column1, column2, errField) = stdApplyLogic(stdOutput, field, nextErrorColumnIndex)
          (column1 :: column2 :: accCols, errField :: accErrorCols, nextErrorColumnIndex + 1)
        }
    }

    (df.select(cols.reverse: _*), errorColNames)
  }

  private def preserveExistingErrorColumn(dataFrame: DataFrame, errorCols: ErrorCols): (DataFrame, ErrorCols) = {
    if (dataFrame.columns.contains(ErrorMessage.errorColumnName)) {
      // preserve existing errors - rename into a temporary column
      val newName = s"${ErrorMessage.errorColumnName}0"
      (dataFrame.withColumnRenamed(ErrorMessage.errorColumnName, newName), newName :: errorCols)
    } else {
      (dataFrame, errorCols)
    }
  }

  private def collectAllTempErrorColumns(dataFrame: DataFrame, errorCols: ErrorCols)
                                        (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame = {
    // collect all of the error attributes into an array and flatten
    logger.info(s"Error cols: $errorCols")
    ArrayTransformations.flattenArrays(
      //reversing errorCols as err columns when collected are appended to beginning of the list
      dataFrame.withColumn(ErrorMessage.errorColumnName, array(errorCols.reverse.map(col): _*)),
      ErrorMessage.errorColumnName)
  }

  private def dropAllTemporaryErrorColumn(dataFrame: DataFrame, errorCols: ErrorCols): DataFrame = {
    dataFrame.drop(errorCols: _*)
  }

  private def cleanTheFinalErrorColumn(dataFrame: DataFrame, errorCols: ErrorCols): DataFrame = {
    dataFrame.withColumn(
      ErrorMessage.errorColumnName, callUDF("cleanErrCol", col(ErrorMessage.errorColumnName))
    )
  }
}
