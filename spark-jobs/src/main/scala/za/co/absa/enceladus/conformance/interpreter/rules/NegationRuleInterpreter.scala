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
import za.co.absa.spark.hats.Extensions._
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, RuleValidators}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, NegationConformanceRule}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.types.GlobalDefaults
import za.co.absa.enceladus.utils.udf.UDFNames
import za.co.absa.enceladus.utils.validation.SchemaPathValidator
import za.co.absa.spark.hats.transformations.NestedArrayTransformations

case class NegationRuleInterpreter(rule: NegationConformanceRule) extends RuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  override def conform(df: Dataset[Row])
                      (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceConfig): Dataset[Row] = {
    NegationRuleInterpreter.validateInputField(progArgs.datasetName, df.schema, rule.inputColumn)

    val field = SchemaUtils.getField(rule.inputColumn, df.schema).get

    val negationErrUdfCall = callUDF(UDFNames.confNegErr, lit(rule.outputColumn), col(rule.inputColumn))
    val errCol = "errCol"

    field.dataType match {
      case _: DecimalType =>
        // Negating decimal cannot fail
        df.nestedMapColumn(rule.inputColumn, rule.outputColumn, c => negate(c))
      case _: BooleanType =>
        // Negating Boolean cannot fail
        df.nestedMapColumn(rule.inputColumn, rule.outputColumn, c => not(c))
      case _: DoubleType | _: FloatType =>
        // Negating floating point numbers cannot fail, but we need to account
        // for signed zeros (see the note for getNegator()).
        df.nestedMapColumn(rule.inputColumn, rule.outputColumn, c => getNegator(c, field))
      case dt =>
        // The generic negation with checking for error conditions
        NestedArrayTransformations.nestedWithColumnAndErrorMap(df, rule.inputColumn, rule.outputColumn, errCol,
          c => getNegator(c, field), c => getError(c, negationErrUdfCall, dt))
    }
  }

  private def getNegator(inputColumn: Column, field: StructField): Column = {
    // Just a couple JVM things:
    // 1. Beware of silent integer overflow, -Int.MinValue == Int.MaxValue + 1 == Int.MinValue
    //    Proof:
    //    a) Int.MaxValue = 2^31 - 1
    //    b) Int.MinValue = -2^31
    //    c) -Int.MinValue = 2^31 = Int.MaxValue + 1 = Int.MinValue
    // 2. Beware negative floating-point zeroes, i.e. 0.0 * -1 == -0.0
    //    Equality (0.0 == -0.0) holds true, but Spark SQL considers 0.0 and -0.0 distinct and fails joins
    // The above is true not only for JVM, but for the most of the CPU/hardware implementations of numeric data types

    def defaultValue(dt: DataType, nullable: Boolean): Any = {
      GlobalDefaults.getDataTypeDefaultValueWithNull(dt, field.nullable).get.orNull
    }

    val neg = negate(inputColumn)
    field.dataType match {
      case _: DoubleType | _: FloatType => when(inputColumn === 0.0, 0.0).otherwise(neg)
      case dt: ByteType => when(inputColumn === Byte.MinValue, defaultValue(dt, field.nullable)).otherwise(neg)
      case dt: ShortType => when(inputColumn === Short.MinValue, defaultValue(dt, field.nullable)).otherwise(neg)
      case dt: IntegerType => when(inputColumn === Int.MinValue,defaultValue(dt, field.nullable)).otherwise(neg)
      case dt: LongType => when(inputColumn === Long.MinValue, defaultValue(dt, field.nullable)).otherwise(neg)
      case _ => neg
    }
  }

  private def getError(inputColumn: Column, errorColumnUDF: Column, fieldType: DataType): Column = {
    fieldType match {
      // scalastyle:off null
      case _: ByteType => when(inputColumn === Byte.MinValue, errorColumnUDF).otherwise(null)
      case _: ShortType => when(inputColumn === Short.MinValue, errorColumnUDF).otherwise(null)
      case _: IntegerType => when(inputColumn === Int.MinValue, errorColumnUDF).otherwise(null)
      case _: LongType => when(inputColumn === Long.MinValue, errorColumnUDF).otherwise(null)
      case a => throw new IllegalArgumentException("NegationRuleInterpreter.getError() should be called only for " +
       s"data types that can produce errors. It is called for $a data type.")
      // scalastyle:on null
    }
  }

}

object NegationRuleInterpreter {

  @throws[ValidationException]
  def validateInputField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val issues = SchemaPathValidator.validateSchemaPathAlgebraic(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName: String, "Negation rule input field is incorrect.", issues)
  }
}
