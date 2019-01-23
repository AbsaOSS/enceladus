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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.conformance.CmdConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.model.conformanceRule.ConcatenationConformanceRule
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations

case class ConcatenationRuleInterpreter(rule: ConcatenationConformanceRule) extends RuleInterpreter {
  final val ruleName = "Concatenation rule"

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    // Validate the rule parameters
    RuleValidators.validateSameParent(progArgs.datasetName, ruleName, rule.inputColumns :+ rule.outputColumn: _*)

    if (rule.outputColumn.contains('.')) {
      conformNestedField(df)
    } else {
      conformRootField(df)
    }
  }

  /** Handles uppercase conformance rule for nested fields. */
  private def conformNestedField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    val parent = rule.inputColumns.head.split('.').dropRight(1).mkString(".")
    DeepArrayTransformations.nestedStructMap(df, parent, rule.outputColumn, c =>
      if (c == null) {
        concat(rule.inputColumns.map(col): _*)
      } else {
        concat(rule.inputColumns.map(a => c.getField(a.split('.').last).cast(StringType)): _*)
      }
    )
  }

  /** Handles uppercase conformance rule for root (non-nested) fields. */
  private def conformRootField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    // Applying the rule
    df.withColumn(rule.outputColumn, concat(rule.inputColumns.map(a => col(a).cast(StringType)): _*))
  }


  /*
  // This is the original implementation. Left it here since it supports concat of fields that have different levels of nestness
  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>
      ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, concat(rule.inputColumns.map(col _): _*))
    }
  }*/
}
