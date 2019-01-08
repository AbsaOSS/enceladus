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
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.NegationConformanceRule
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.utils.validation.{SchemaPathValidator, ValidationIssue, ValidationUtils}

case class NegationRuleInterpreter(rule: NegationConformanceRule) extends RuleInterpreter {

  override def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>
      NegationRuleInterpreter.validateInputField(df.schema, rule.inputColumn)
      val inputColumn = col(rule.inputColumn)
      val fieldType = SchemaUtils.getFieldType(rule.inputColumn, df.schema).get

      // Just a couple JVM things:
      // 1. Beware of silent integer overflow, -Int.MinValue == Int.MaxValue + 1 == Int.MinValue
      //    Proof:
      //    a) Int.MaxValue = 2^31 - 1
      //    b) Int.MinValue = -2^31
      //    c) -Int.MinValue = 2^31 = Int.MaxValue + 1 = Int.MinValue
      val negationErrUdfCall = callUDF("confNegErr", lit(rule.outputColumn), col(rule.inputColumn))
      val appendErrUdfCall = callUDF("errorColumnAppend", col(ErrorMessage.errorColumnName), negationErrUdfCall)

      // 2. Beware negative floating-point zeroes, i.e. 0.0 * -1 == -0.0
      //    Equality (0.0 == -0.0) holds true, but Spark SQL considers 0.0 and -0.0 distinct and fails joins
      val neg = negate(inputColumn)
      val typeSpecificNegate = fieldType match {
        case _: DoubleType | _: FloatType => when(inputColumn === 0.0, 0.0).otherwise(neg)
        case dt: ByteType                 => when(inputColumn === Byte.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
        case dt: ShortType                => when(inputColumn === Short.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
        case dt: IntegerType              => when(inputColumn === Int.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
        case dt: LongType                 => when(inputColumn === Long.MinValue, Defaults.getGlobalDefault(dt)).otherwise(neg)
        case _                            => neg
      }

      val negated = ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, typeSpecificNegate)
      negated.withColumn(ErrorMessage.errorColumnName, getError(inputColumn, appendErrUdfCall, fieldType))
    }
  }

  private def getError(inputColumn: Column, appendErrUdfCall: Column, fieldType: DataType) = {
    fieldType match {
      case _: ByteType    => when(inputColumn === Byte.MinValue, appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName))
      case _: ShortType   => when(inputColumn === Short.MinValue, appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName))
      case _: IntegerType => when(inputColumn === Int.MinValue, appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName))
      case _: LongType    => when(inputColumn === Long.MinValue, appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName))
      case _              => col(ErrorMessage.errorColumnName)
    }
  }

}

object NegationRuleInterpreter {

  @throws[ValidationException]
  def validateInputField(schema: StructType, fieldPath: String): Unit = {
    val issues = SchemaPathValidator.validateSchemaPathNumeric(schema, fieldPath)
    checkAndThrowValidationErrors("Negation rule input field is incorrect.", issues)
  }

  @throws[ValidationException]
  private def checkAndThrowValidationErrors(message: String, validationIssues: Seq[ValidationIssue]): Unit = {
    if (validationIssues.nonEmpty) {
      val errorMeaasges = ValidationUtils.getValidationMsgs(validationIssues).mkString(";")
      throw new ValidationException(s"$message $errorMeaasges" )
    }
  }

}
