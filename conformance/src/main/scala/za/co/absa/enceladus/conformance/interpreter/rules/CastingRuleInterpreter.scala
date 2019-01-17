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

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.CastingConformanceRule
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.validation._

case class CastingRuleInterpreter(rule: CastingConformanceRule) extends RuleInterpreter {
  final val ruleName = "Casting rule"

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    // Validate the rule parameters
    RuleValidators.validateInputField(ruleName, progArgs.datasetName, df.schema, rule.inputColumn)
    RuleValidators.validateOutputField(ruleName, progArgs.datasetName, df.schema, rule.outputColumn)
    RuleValidators.validateSameParent(ruleName, rule.inputColumn, rule.outputColumn)

    if (rule.inputColumn.contains('.')) {
      conformNestedField(df)
    } else {
      conformRootField(df)
    }
  }

  /** Handles casting conformance rule for nested fields. Could be used for non-nested fields as well, but it would be much slower. */
  private def conformNestedField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    val idField = rule.outputColumn.replace(".", "_") + "_arrayConformanceId"
    val withUniqueId = df.withColumn(idField, monotonically_increasing_id())
    var errorsDf = df

    val res = handleArrays(rule.outputColumn, withUniqueId) { flattened =>
      // UDFs for appending a conformance casting error to the error column
      val castingErrUdfCall = callUDF("confCastErr", lit(rule.outputColumn), col(rule.inputColumn).cast(StringType))
      val appendErrUdfCall = callUDF("errorColumnAppend", col(ErrorMessage.errorColumnName), castingErrUdfCall)

      // Applying the casting rule
      val casted = ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, col(rule.inputColumn).cast(rule.outputDataType))

      // Casting error is appended if cast() produced null while the original column value is not null
      // casted.filter((col("items['itemid']") isNotNull) and (col("items['ConformedId']") isNull) ).take(100)
      errorsDf = casted.withColumn(
        ErrorMessage.errorColumnName,
        when((col(rule.outputColumn) isNull) and (col(rule.inputColumn) isNotNull), appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName)))

      casted
    }

    val errNested = errorsDf.groupBy(idField).agg(collect_list(col(ErrorMessage.errorColumnName)) as ErrorMessage.errorColumnName)
    val errNestedSchema = SchemaUtils.getFieldType(ErrorMessage.errorColumnName, errNested.schema).get.asInstanceOf[ArrayType]

    spark.udf.register(s"${idField}_flattenErrDistinct", new UDF1[Seq[Seq[Row]], Seq[Row]] {
      override def call(t1: Seq[Seq[Row]]) = {
        // Remove duplicate errors that were caused by the array explosion
        t1.flatten.distinct
      }

    }, errNestedSchema.elementType)

    val withErr = errNested.withColumn(ErrorMessage.errorColumnName, expr(s"${idField}_flattenErrDistinct(${ErrorMessage.errorColumnName})"))

    // Join on the errors
    val res2 = res.drop(ErrorMessage.errorColumnName).as("conf")
      .join(withErr.as("err"), col(s"conf.$idField") === col(s"err.$idField"), "left_outer").select(col("conf.*"), col(s"err.${ErrorMessage.errorColumnName}")).drop(idField)

    res2
  }

  /** Handles casting conformance rule for root (non-nested) fields. It has much better performance than the more generic `conformNestedField` implementation. */
  private def conformRootField(df: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>
      // UDFs for appending a conformance casting error to the error column
      val castingErrUdfCall = callUDF("confCastErr", lit(rule.outputColumn), col(rule.inputColumn).cast(StringType))
      val appendErrUdfCall = callUDF("errorColumnAppend", col(ErrorMessage.errorColumnName), castingErrUdfCall)

      // Applying the casting rule
      val casted = ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, col(rule.inputColumn).cast(rule.outputDataType))
      val errorsDf = casted.withColumn(
        ErrorMessage.errorColumnName,
        when((col(rule.outputColumn) isNull) and (col(rule.inputColumn) isNotNull), appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName)))

      errorsDf
    }
  }

}
