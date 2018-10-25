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

package za.co.absa.enceladus.utils.validation.field

import org.apache.spark.sql.types._
import za.co.absa.enceladus.utils.validation.ValidationIssue

trait FieldValidator {
  def validateStructField(field: StructField): Seq[ValidationIssue]
}

object FieldValidator {

  /**
    * Validates a StructField and returns the list of issues encountered
    *
    * @param field A field
    * @return The list if validation issues encountered
    */
  def validate(field: StructField): Seq[ValidationIssue] = {
    val validator = field.dataType match {
      case _: DateType =>
        Some(new FieldValidatorDate)
      case _: TimestampType =>
        Some(new FieldValidatorTimestamp)
      case _: BooleanType =>
        Some(new FieldValidatorScalar)
      case _: NumericType =>
        Some(new FieldValidatorScalar)
      case _: StringType =>
        Some(new FieldValidatorScalar)
      case _ => None
    }
    validator.map(_.validateStructField(field)).getOrElse(Nil)
  }
}
