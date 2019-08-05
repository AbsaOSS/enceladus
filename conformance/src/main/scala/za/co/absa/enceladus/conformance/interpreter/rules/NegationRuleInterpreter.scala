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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.NegationConformanceRule
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.utils.validation.SchemaPathValidator

case class NegationRuleInterpreter(rule: NegationConformanceRule) extends RuleInterpreter {

  override def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    NegationRuleInterpreter.validateInputField(progArgs.datasetName, df.schema, rule.inputColumn)

    val fieldType = SchemaUtils.getFieldType(rule.inputColumn, df.schema).get
    val negationErrUdfCall = callUDF("confNegErr", lit(rule.outputColumn), col(rule.inputColumn))
    val errCol = "errCol"

    fieldType match {
      case _: DecimalType =>
        // Negating decimal cannot fail
        DeepArrayTransformations.nestedWithColumnMap(df, rule.inputColumn, rule.outputColumn, c => negate(c))
      case _: DoubleType | _: FloatType =>
        // Negating floating point numbers cannot fail, but we need to account for signed zeros (see the note for getNegator()).
        DeepArrayTransformations.nestedWithColumnMap(df, rule.inputColumn, rule.outputColumn, c => getNegator(c, fieldType))
      case _ =>
        // The generic negation with checking for error conditions
        DeepArrayTransformations.nestedWithColumnAndErrorMap(df, rule.inputColumn, rule.outputColumn, errCol,
          c => getNegator(c, fieldType), c => getError(c, negationErrUdfCall, fieldType))
    }
  }

  private def getNegator(inputColumn: Column, inputDataType: DataType): Column = {
    // Just a couple JVM things:
    // 1. Beware of silent integer overflow, -Int.MinValue == Int.MaxValue + 1 == Int.MinValue
    //    Proof:
    //    a) Int.MaxValue = 2^31 - 1
    //    b) Int.MinValue = -2^31
    //    c) -Int.MinValue = 2^31 = Int.MaxValue + 1 = Int.MinValue
    // 2. Beware negative floating-point zeroes, i.e. 0.0 * -1 == -0.0
    //    Equality (0.0 == -0.0) holds true, but Spark SQL considers 0.0 and -0.0 distinct and fails joins
    // The above is true not only for JVM, but for the most of the CPU/hardware implementations of numeric data types
    val neg = negate(inputColumn)
    inputDataType match {
      case _: DoubleType | _: FloatType => when(inputColumn === 0.0, 0.0).otherwise(neg)
      case dt: ByteType => when(inputColumn === Byte.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
      case dt: ShortType => when(inputColumn === Short.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
      case dt: IntegerType => when(inputColumn === Int.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
      case dt: LongType => when(inputColumn === Long.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
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
      case _ => null
      // scalastyle:on null
    }
  }

}

object NegationRuleInterpreter {

  @throws[ValidationException]
  def validateInputField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val issues = SchemaPathValidator.validateSchemaPathNumeric(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName: String, "Negation rule input field is incorrect.", issues)
  }
}
