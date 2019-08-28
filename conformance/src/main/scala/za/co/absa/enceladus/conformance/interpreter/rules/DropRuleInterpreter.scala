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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.DropConformanceRule
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations

import scala.util.{Failure, Success, Try}

case class DropRuleInterpreter(rule: DropConformanceRule) extends RuleInterpreter {

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    Try(df(rule.outputColumn)) match {
      case Success(_) =>
        if (rule.outputColumn.contains('.')) {
          conformNestedField(df)
        } else {
          conformRootField(df)
        }
      case Failure(_) =>
        log.warn(s"Could not drop ${rule.outputColumn}. Column is not present in the dataset")
        df
    }
  }

  /** Handles drop conformance rule for nested fields. */
  private def conformNestedField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    DeepArrayTransformations.nestedDropColumn(df, rule.outputColumn)
  }

  /** Handles drop conformance rule for root (non-nested) fields. */
  private def conformRootField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    // Applying the rule
    df.drop(rule.outputColumn)
  }
}
