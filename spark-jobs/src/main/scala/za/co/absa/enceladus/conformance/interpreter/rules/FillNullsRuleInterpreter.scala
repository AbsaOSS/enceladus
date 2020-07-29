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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, RuleValidators}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, FillNullsConformanceRule}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.spark.hats.Extensions._

import scala.util.{Failure, Success}

object FillNullsRuleInterpreter {
  final val ruleName = "Fill Nulls Rule"
}

case class FillNullsRuleInterpreter(rule: FillNullsConformanceRule) extends RuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceConfig): Dataset[Row] = {
    // Validate the rule parameters
    RuleValidators.validateOutputField(
      progArgs.datasetName,
      FillNullsRuleInterpreter.ruleName,
      df.schema,
      rule.outputColumn
    )

    val dataType: DataType = SchemaUtils.getFieldType(rule.inputColumn, df.schema).get
    val default: Column = simpleLiteralCast(rule.value, dataType) match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ValidationException(
          s"""Unable to cast literal ${rule.value} to $dataType
             | for FillNulls conformance rule number ${rule.order}.""".stripMargin.replaceAll("[\\r\\n]", ""),
          cause = exception)
    }

    if (rule.outputColumn.contains('.')) {
      conformNestedField(df, default)
    } else {
      conformRootField(df, default)
    }
  }

  private def conformNestedField(df: Dataset[Row], default: Column)(implicit spark: SparkSession): Dataset[Row] = {
    df.nestedWithColumnExtended(rule.outputColumn, getField => coalesce(getField(rule.inputColumn), default))
  }

  private def conformRootField(df: Dataset[Row], default: Column)(implicit spark: SparkSession): Dataset[Row] = {
    df.withColumn(rule.outputColumn, coalesce(col(rule.inputColumn), default))
  }
}
