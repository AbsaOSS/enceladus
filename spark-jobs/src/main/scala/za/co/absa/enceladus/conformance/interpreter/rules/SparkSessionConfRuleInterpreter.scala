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
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.spark.hats.Extensions._
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, RuleValidators}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, SparkSessionConfConformanceRule}

case class SparkSessionConfRuleInterpreter(rule: SparkSessionConfConformanceRule) extends RuleInterpreter {

  final val ruleName = "Spark Session Config rule"

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceConfig): Dataset[Row] = {
    // Validate the rule parameters
    RuleValidators.validateOutputField(ruleName, progArgs.datasetName, df.schema, rule.outputColumn)

    if (rule.outputColumn.contains('.')) {
      conformNestedField(df)
    } else {
      conformRootField(df)
    }
  }

  /** Handles Spark session config conformance rule for nested fields. */
  private def conformNestedField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    val configValue = spark.sessionState.conf.getConfString(rule.sparkConfKey)
    df.nestedWithColumn(rule.outputColumn, inferStrictestType(configValue))
  }

  /** Handles Spark session config conformance rule for root (non-nested) fields. */
  private def conformRootField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    // Applying the rule
    val configValue = spark.sessionState.conf.getConfString(rule.sparkConfKey)
    df.withColumn(rule.outputColumn, inferStrictestType(configValue))
  }

}
