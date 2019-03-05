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

package za.co.absa.enceladus.utils.validation.field

import org.apache.spark.sql.types.StructField
import za.co.absa.enceladus.utils.validation._

import scala.collection.mutable.ListBuffer

class FieldValidatorTimestamp extends FieldValidator {
  override def validateStructField(field: StructField): Seq[ValidationIssue] = {
    val issues = new ListBuffer[ValidationIssue]
    if (field.metadata contains "pattern") {
      val pattern = field.metadata.getString("pattern")
      val default = if (field.metadata contains "default") Some(field.metadata.getString("default")) else None
      val patternError = DateTimeValidator.isDateTimePatternValid(pattern, default)
      if (patternError.nonEmpty) {
        issues += patternError.get
      }
      else {
        val logicalErrors = isTimestampPatternGood(pattern)
        issues ++= logicalErrors
      }
    }
    issues
  }

  /**
    * Checks if a timestamp pattern is good, i.e., no required fields are missing
    *
    * @param pattern A timestamp pattern
    * @return None if no validation errors or Some(String) an error message
    */
  def isTimestampPatternGood(pattern: String): Seq[ValidationIssue] = {
    if (".*[Hms].*".r.findFirstIn(pattern).isEmpty)
      return List(ValidationError("Placeholders for hour, minute and second (H,m,s) not found. Possibly 'Date' type intended."))

    val issues = new ListBuffer[ValidationIssue]
    if (!pattern.contains('y'))
      issues += ValidationWarning("No year placeholder 'yyyy' found.")
    if (!pattern.contains('M'))
      issues += ValidationWarning("No month placeholder 'MM' found.")
    if (!pattern.contains('d'))
      issues += ValidationWarning("No day placeholder 'dd' found.")

    if (!pattern.contains('H'))
      issues += ValidationWarning("No hour placeholder 'HH' found.")
    if (!pattern.contains('m'))
      issues += ValidationWarning("No minute placeholder 'mm' found.")
    if (!pattern.contains('s'))
      issues += ValidationWarning("No second placeholder 'ss' found.")

    if (pattern.contains('D'))
      issues += ValidationWarning("Rarely used DayOfYear placeholder 'D' found. Possibly DayOfMonth 'd' intended.")

    issues.toList
  }

}
