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

package za.co.absa.enceladus.conformance.interpreter.rules.mapping

import org.apache.spark.sql.types.StructType
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.utils.validation.SchemaPathValidator

trait JoinMappingRuleInterpreter extends CommonMappingRuleInterpreter {
  override protected def validateMappingFieldsExist(joinConditionStr:  String,
                                                    datasetSchema: StructType,
                                                    mappingTableSchema: StructType,
                                                    rule: MappingConformanceRule): Unit = {
    JoinMappingRuleInterpreter.validateMappingFieldsExist(datasetWithJoinCondition(joinConditionStr),
                                                          datasetSchema,
                                                          mappingTableSchema,
                                                          rule)
  }

  private def datasetWithJoinCondition(joinConditionStr: String): String = {
    s"the dataset, join condition = $joinConditionStr"
  }
}

object JoinMappingRuleInterpreter {

  @throws[ValidationException]
  def validateMappingFieldsExist(datasetName: String,
                                                datasetSchema: StructType,
                                                mappingTableSchema: StructType,
                                                rule: MappingConformanceRule): Unit = {
    // Validate the output column
    validateOutputField(datasetName, datasetSchema, rule.outputColumn)

    // Validate the target attribute
    validateTargetAttribute(rule.mappingTable, mappingTableSchema, rule.targetAttribute)

    // Validate the join condition
    rule.attributeMappings.foreach {
      case (mappingTableField, datasetField) =>
        validateJoinField(rule.mappingTable, mappingTableSchema, mappingTableField)
        validateJoinField(datasetName, datasetSchema, datasetField)
    }
  }

  @throws[ValidationException]
  private def validateJoinField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName, "A join condition validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  private def validateTargetAttribute(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName, "A tagret attribute validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  private def validateOutputField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPathParent(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName, "An output column name validation error occurred.", validationIssues)
  }
}
