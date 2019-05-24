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

import java.util.Date

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, TimestampType}
import za.co.absa.enceladus.utils.time.{DateTimePattern, EnceladusDateTimeParser}
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.utils.validation._

object FieldValidatorTimestamp extends FieldValidatorDateTime {
  import za.co.absa.enceladus.utils.implicits.StringImplicits.StringEnhancements

  override protected def patternAnalysisIssues(pattern: DateTimePattern,
                                               defaultValue: Option[String],
                                               defaultTimeZone: Option[String]): Seq[ValidationIssue] = {

    val doubleTimeZoneIssue: Seq[ValidationIssue] = if (pattern.timeZoneInPattern && defaultTimeZone.nonEmpty) {
      Seq(ValidationWarning(
        "Pattern includes time zone placeholder and default time zone is also defined (will never be used)"
      ))
    } else {
      Nil
    }

    val patternIssues: Seq[ValidationIssue] = if (!pattern.isEpoch) {
      val placeholders = Set('y', 'M', 'd', 'H', 'm', 's', 'D', 'K', 'h', 'a', 'k')
      val patternChars = pattern.pattern.countUnquoted(placeholders, Set('''))
      patternChars.foldLeft(List.empty[ValidationIssue]) {(acc, item) => item match {
        case ('y', 0) => ValidationWarning("No year placeholder 'yyyy' found.")::acc
        case ('M', 0) => ValidationWarning("No month placeholder 'MM' found.")::acc
        case ('d', 0) => ValidationWarning("No day placeholder 'dd' found.")::acc
        case ('H', 0) if patternChars('k') + patternChars('K') + patternChars('h') == 0 =>
          ValidationWarning("No hour placeholder 'HH' found.")::acc
        case ('m', 0) => ValidationWarning("No minute placeholder 'mm' found.")::acc
        case ('s', 0) => ValidationWarning("No second placeholder 'ss' found.")::acc
        case ('D', x) if x > 0 && patternChars('d') == 0 =>
          ValidationWarning("Rarely used DayOfYear placeholder 'D' found. Possibly DayOfMonth 'd' intended.")::acc
        case ('h', x) if x > 0 && patternChars('a') == 0 =>
          ValidationWarning(
            "Placeholder for hour 1-12 'h' found, but no am/pm 'a' placeholder. Possibly 0-23 'H' intended."
          )::acc
        case ('K', x) if x > 0 && patternChars('a') == 0 =>
          ValidationWarning(
            "Placeholder for hour 0-11 'K' found, but no am/pm 'a' placeholder. Possibly 1-24 'k' intended."
          )::acc
        case _ => acc
      }}
    } else {
      Nil
    }
    doubleTimeZoneIssue ++ patternIssues
  }

  override def verifyStringDateTime(dateTime: String)(implicit parser: EnceladusDateTimeParser): Date = {
    parser.parseTimestamp(dateTime)
  }

}
