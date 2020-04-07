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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.spark.hats.Extensions._
import za.co.absa.enceladus.conformance.ConfCmdConfig
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, RuleValidators}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{CastingConformanceRule, ConformanceRule}
import za.co.absa.enceladus.utils.error.UDFNames
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.spark.hats.transformations.NestedArrayTransformations

case class CastingRuleInterpreter(rule: CastingConformanceRule) extends RuleInterpreter {

  final val ruleName = "Casting rule"

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConfCmdConfig): Dataset[Row] = {
    // Validate the rule parameters
    RuleValidators.validateInputField(progArgs.datasetName, ruleName, df.schema, rule.inputColumn)
    RuleValidators.validateOutputField(progArgs.datasetName, ruleName, df.schema, rule.outputColumn)
    RuleValidators.validateSameParent(progArgs.datasetName, ruleName, rule.inputColumn, rule.outputColumn)

    SchemaUtils.getFieldType(rule.inputColumn, df.schema)
      .foreach(dt => RuleValidators.validateTypeCompatibility(ruleName, rule.inputColumn, dt, rule.outputDataType))

    val sourceDataType = SchemaUtils.getFieldType(rule.inputColumn, df.schema).get
    val targetDataType = CatalystSqlParser.parseDataType(rule.outputDataType)

    if (SchemaUtils.isCastAlwaysSucceeds(sourceDataType, targetDataType)) {
      // Casting to string does not generate errors
      df.nestedMapColumn(rule.inputColumn, rule.outputColumn, c =>
        c.cast(rule.outputDataType)
      )
    } else {
      NestedArrayTransformations.nestedWithColumnAndErrorMap(df, rule.inputColumn, rule.outputColumn, "errCol",
        c => {
          c.cast(rule.outputDataType)
        }, c => {
          when(c.isNotNull.and(c.cast(rule.outputDataType).isNull),
            callUDF(UDFNames.confCastErr, lit(rule.outputColumn), c.cast(StringType)))
            .otherwise(null) // scalastyle:ignore null
        })
    }
  }

}
