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

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.spark.hats.Extensions._
import za.co.absa.enceladus.conformance.ConfCmdConfig
import za.co.absa.enceladus.conformance.interpreter.ExplosionState
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, DropConformanceRule}
import za.co.absa.enceladus.utils.schema.SchemaUtils

case class DropRuleInterpreter(rule: DropConformanceRule) extends RuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConfCmdConfig): Dataset[Row] = {
    if (SchemaUtils.fieldExists(rule.outputColumn, df.schema)) {
      if (rule.outputColumn.contains('.')) {
        conformNestedField(df)
      } else {
        conformRootField(df)
      }
    } else {
      log.warn(s"Could not drop ${rule.outputColumn}. Column is not present in the dataset")
      df
    }
  }

  /** Handles drop conformance rule for nested fields. */
  private def conformNestedField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    df.nestedDropColumn(rule.outputColumn)
  }

  /** Handles drop conformance rule for root (non-nested) fields. */
  private def conformRootField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    // Applying the rule
    df.drop(rule.outputColumn)
  }
}
