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
package za.co.absa.enceladus.examples.interpreter.rules.custom

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.common.cmd.ConformanceCmdConfig
import za.co.absa.enceladus.conformance.interpreter.ExplosionState
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.utils.transformations.ArrayTransformations

case class UppercaseCustomConformanceRule(
  order: Int,
  outputColumn: String,
  controlCheckpoint: Boolean,
  inputColumn: String
) extends CustomConformanceRule  {
  override def getInterpreter(): RuleInterpreter = UppercaseCustomRuleInterpreter(this)

  override def withUpdatedOrder(newOrder: Int): conformanceRule.ConformanceRule = copy(order = newOrder)
}

case class UppercaseCustomRuleInterpreter(rule: UppercaseCustomConformanceRule) extends RuleInterpreter {
  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceCmdConfig): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>

      // we have to do this if this rule is to support arrays
      ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, upper(col(rule.inputColumn)))
    }
  }

}
