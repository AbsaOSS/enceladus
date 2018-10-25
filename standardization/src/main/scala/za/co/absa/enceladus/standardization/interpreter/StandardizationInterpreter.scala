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

import org.apache.spark.sql.{ Dataset, Row, Column, SparkSession }
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import za.co.absa.enceladus.standardization.interpreter.dataTypes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.SchemaUtils
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.schema.SchemaUtils
import org.slf4s.LoggerFactory
import za.co.absa.enceladus.utils.validation.ValidationException
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.standardization.interpreter.stages.SchemaChecker
import za.co.absa.enceladus.standardization.interpreter.stages.SparkXMLHack
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParser
import scala.util.Random
import scala.collection.mutable.Buffer

/**
 * Object representing set of tools for performing the actual standardization
 */
object StandardizationInterpreter {

  val logger = LoggerFactory.getLogger(this.getClass)

  private var errorCols: Buffer[String] = null
  private def unpath(path: String): String = path.replace('.', '_')

  
  // This helper fn defines the standardization logic used with arrays and normally
  def stdApplyLogic(stdOutput: ParseOutput, field: StructField, df: Dataset[Row])(implicit spark: SparkSession, udfLib: UDFLibrary): Dataset[Row] = {

    val ParseOutput(stdCol, errs) = stdOutput
    
    val errField = s"error_${unpath(field.name)}_${Random.nextInt().abs}"
    errorCols.append(errField)
    
    val inclErrs = df.withColumn(errField, errs)

    // If the meta data value sourcecolumn is set override the field name
    val fieldName = SchemaUtils.getFieldNameOverriddenByMetadata(field)

    inclErrs.withColumn(fieldName, stdCol)
  }

  /**
   * Perform the standardization of the dataframe given the expected schema
   * @param df Dataframe to be standardized
   * @param expSchema The schema for the df to be standardized into
   */
  def standardize(df: Dataset[Row], expSchema: StructType, inputType: String)(implicit spark: SparkSession, udfLib: UDFLibrary): Dataset[Row] = {
    import spark.implicits._

    errorCols = ListBuffer[String]()

    // Step 1 Schema validation against self inconsistencies
    logger.info(s"Step 1: Schema validation")
    val validationErrors = SchemaChecker.validateSchemaAndLog(expSchema)
    if (validationErrors._1.nonEmpty) {
      throw new ValidationException("A fatal schema validation error occurred.", validationErrors._1)
    }

    // TODO: remove when spark-xml handles empty arrays
    logger.info(s"Step 1.5: Spark-xml hack (handle empty arrays)")
    val df1 = if (inputType.toLowerCase() == "xml") {
      expSchema.fields.foldLeft(df)({
        case (accDf: Dataset[Row], field: StructField) => {
          val newType = SparkXMLHack.hack(field, "", accDf)
          accDf.withColumn(field.name, newType)
        }

      })
    } else df

    logger.info(s"Step 2: Standardization")
    // step 2 - standardize
    val std = expSchema.fields.foldLeft(df1)({
      case (accDf: Dataset[Row], field: StructField) => {

        logger.info(s"Standardizing field: ${field.name}")

        // Do not standardize existing error column
        if (field.name == ErrorMessage.errorColumnName) {
          accDf
        } else {
          val stdOutput = TypeParser.standardize(field, "", df.schema)
          logger.info(s"Applying standardization plan for ${field.name}")
          val res = stdApplyLogic(stdOutput, field, accDf)
          res
        }
      }
    })

    // step 3 drop intermediate error columns
    logger.info(s"Step 3.1: Preserve existing error column")
    // preserve existing errors - rename into a temporary column
    val errColPreserved = if (std.columns.contains(ErrorMessage.errorColumnName)) {
      val newName = ErrorMessage.errorColumnName + (Random.nextInt.abs)
      errorCols.append(newName)
      std.withColumnRenamed(ErrorMessage.errorColumnName, newName)
    } else std

    logger.info(s"Step 3.2: Collect all temporary error columns")
    // collect all of the error attributes into an array and flatten
    logger.info(s"Error cols: ${errorCols.toList}")
    val collectedError = ArrayTransformations.flattenArrays(
      errColPreserved.withColumn(ErrorMessage.errorColumnName, array(errorCols.map(col(_)): _*)),
      ErrorMessage.errorColumnName)

    logger.info(s"Step 3.3: Drop all temporary error columns")
    val droppedErrs = collectedError.drop(errorCols.distinct: _*)
    logger.info(s"Step 3.4: Clean the final error column")
    val cleanedErr = droppedErrs.withColumn(ErrorMessage.errorColumnName, callUDF("cleanErrCol", col(ErrorMessage.errorColumnName)))

    logger.info(s"Standardization process finished, returning to the application...")
    cleanedErr
  }

}