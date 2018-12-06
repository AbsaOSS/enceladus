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
package src.main.scala.za.co.absa.enceladus.conformanceExamples.interpreter.rules.custom

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import org.apache.spark.sql.functions._

case class UppercaseCustomConformanceRule(
  order: Int,
  outputColumn: String,
  controlCheckpoint: Boolean,
  inputColumn: String
) extends CustomConformanceRule  {
  override def getInterpreter(): RuleInterpreter = UppercaseCustomRuleInterpreter(this)
}

case class UppercaseCustomRuleInterpreter(rule: UppercaseCustomConformanceRule) extends RuleInterpreter {

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>
      import spark.implicits._
      // we have to do this if this rule is to support arrays
      ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, upper(col(rule.inputColumn)))
    }
  }

}
