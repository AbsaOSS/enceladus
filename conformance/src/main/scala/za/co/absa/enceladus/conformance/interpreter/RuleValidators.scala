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

package za.co.absa.enceladus.conformance.interpreter

import org.apache.spark.sql.types.StructType
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.utils.validation.{SchemaPathValidator, ValidationIssue, ValidationUtils}

object RuleValidators {

  @throws[ValidationException]
  def validateInputField(ruleName: String, datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val existenceIssues = SchemaPathValidator.validateSchemaPath(datasetName, schema, fieldPath)
    val primitivityIssues = SchemaPathValidator.validateSchemaPathPrimitive(schema, fieldPath)
    checkAndThrowValidationErrors(s"$ruleName validation error: input field is incorrect.", existenceIssues ++ primitivityIssues)
  }

  @throws[ValidationException]
  def validateOutputField(ruleName: String, datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val issues = SchemaPathValidator.validateSchemaPathOutput(datasetName, schema, fieldPath)
    checkAndThrowValidationErrors(s"$ruleName validation error: output field is incorrect.", issues)
  }

  @throws[ValidationException]
  def validateSameParent(ruleName: String, fieldPath1: String, fieldPath2: String): Unit = {
    val issues = SchemaPathValidator.validatePathSameParent(fieldPath1, fieldPath2)
    checkAndThrowValidationErrors(s"$ruleName validation error: input and output columns don't have the same parent.", issues)
  }

  @throws[ValidationException]
  private def checkAndThrowValidationErrors(message: String, validationIssues: Seq[ValidationIssue]): Unit = {
    if (validationIssues.nonEmpty) {
      val errorMeaasges = ValidationUtils.getValidationMsgs(validationIssues).mkString(";")
      throw new ValidationException(s"$message $errorMeaasges" )
    }
  }
}
