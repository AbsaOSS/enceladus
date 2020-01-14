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

package za.co.absa.enceladus.conformance.interpreter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.utils.validation.{SchemaPathValidator, ValidationIssue, ValidationUtils}

import scala.util.control.NonFatal

object RuleValidators {

  @throws[ValidationException]
  def validateInputField(datasetName: String, ruleName: String, schema: StructType, fieldPath: String): Unit = {
    val existenceIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    val primitivityIssues = SchemaPathValidator.validateSchemaPathPrimitive(schema, fieldPath)
    checkAndThrowValidationErrors(datasetName, s"$ruleName validation error: input field is incorrect.", existenceIssues ++ primitivityIssues)
  }

  @throws[ValidationException]
  def validateFieldExistence(datasetName: String, ruleName: String, schema: StructType, fieldPaths: String*): Unit = {
    val existenceIssues = fieldPaths.flatMap(field => SchemaPathValidator.validateSchemaPath(schema, field))
    checkAndThrowValidationErrors(datasetName, s"$ruleName validation error: input field does not exist.", existenceIssues)
  }

  @throws[ValidationException]
  def validateOutputField(datasetName: String, ruleName: String, schema: StructType, fieldPath: String): Unit = {
    val issues = SchemaPathValidator.validateSchemaPathOutput(schema, fieldPath)
    checkAndThrowValidationErrors(datasetName, s"$ruleName validation error: output field is incorrect.", issues)
  }

  @throws[ValidationException]
  def validateSameParent(datasetName: String, ruleName: String, fieldPaths: String*): Unit = {
    val firstField = fieldPaths.head
    val issues =  fieldPaths.tail.flatMap(field => SchemaPathValidator.validatePathSameParent(firstField, field))
    checkAndThrowValidationErrors(datasetName, s"$ruleName validation error: input and output columns don't have the same parent.", issues)
  }

  @throws[ValidationException]
  def checkAndThrowValidationErrors(datasetName: String, message: String, validationIssues: Seq[ValidationIssue]): Unit = {
    if (validationIssues.nonEmpty) {
      val errorMessages = ValidationUtils.getValidationMsgs(validationIssues).mkString(";")
      throw new ValidationException(s"$datasetName - $message $errorMessages" )
    }
  }

  /**
    * Checks if Spark allows casting from one specific type to another.
    * Throws an instance of ValidationException exception otherwise.
    *
    * @param ruleName        A name of a conformance rule to be used as part of an exception error message
    * @param inputColumnName A name of an input column to be used as part of an exception error message
    * @param inputType       A type of an input field
    * @param outputTypeName  A name oa a type definition to cast to
    * @param spark           (implicit) A Spark Session
    */
  @throws[ValidationException]
  def validateTypeCompatibility(ruleName: String,
                                inputColumnName: String,
                                inputType: DataType,
                                outputTypeName: String)
                               (implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // This generates a dataframe we can use to check cast()
    // Various values need to be here, otherwise Spark may accept incompatible types
    val df = List("", "true", "false", "0", "1", "-999", "2.2", "a",
      "2019-01-01", "2019-01-01 00:00:00", "1970-01-01 00:00:00").toDF("dummy")

    // Check if inputType can be cast to outputType
    // If types are completely incompatible Spark throws an exception
    try {
      // The initial type is 'string'. Convert it to 'inputType' first (conversion from string never throws), and than
      // try to convert it to 'outputType'.
      df.select(col("dummy"), col("dummy").cast(inputType).cast(outputTypeName).as("field"))
        .collect
    } catch {
      case NonFatal(e) =>
        throw new ValidationException(
          s"$ruleName validation error: cannot cast '$inputColumnName' to '$outputTypeName' since conversion from " +
            s"'${inputType.typeName}' to '$outputTypeName' is not supported.", e.getMessage)
    }
  }
}
