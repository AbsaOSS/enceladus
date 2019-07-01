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
import org.apache.spark.sql.types.TimestampType
import za.co.absa.enceladus.utils.time.{DateTimePattern, EnceladusDateTimeParser}
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.utils.validation._

object FieldValidatorDate extends FieldValidatorDateTime {
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

    val timeZoneIssue: Option[ValidationIssue] = if (!pattern.isEpoch && pattern.isTimeZoned) {
      Option(ValidationWarning(
        "Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes."
      ))
    } else {
      None
    }

    val patternIssues: Seq[ValidationIssue] = if (!pattern.isEpoch) {
      val placeholders = Set('y', 'M', 'd', 'H', 'm', 's', 'D', 'S', 'a', 'k', 'K', 'h')
      val patternChars = pattern.pattern.countUnquoted(placeholders, Set('''))
      patternChars.foldLeft(List.empty[ValidationIssue]) {(acc, item) => item match {
        case ('y', 0) => ValidationWarning("No year placeholder 'yyyy' found.")::acc
        case ('M', 0) => ValidationWarning("No month placeholder 'MM' found.")::acc
        case ('d', 0) => ValidationWarning("No day placeholder 'dd' found.")::acc
        case ('H', x) if x > 0 => ValidationWarning("Redundant hour placeholder 'H' found.")::acc
        case ('m', x) if x > 0 => ValidationWarning("Redundant minute placeholder 'm' found.")::acc
        case ('s', x) if x > 0 => ValidationWarning("Redundant second placeholder 's' found.")::acc
        case ('S', x) if x > 0 => ValidationWarning("Redundant millisecond placeholder 'S' found.")::acc
        case ('a', x) if x > 0 => ValidationWarning("Redundant am/pm placeholder 'a' found.")::acc
        case ('k', x) if x > 0 => ValidationWarning("Redundant hour placeholder 'k' found.")::acc
        case ('h', x) if x > 0 => ValidationWarning("Redundant hour placeholder 'h' found.")::acc
        case ('K', x) if x > 0 => ValidationWarning("Redundant hour placeholder 'H' found.")::acc
        case ('D', x) if x > 0 && patternChars('d') == 0  =>
          ValidationWarning("Rarely used DayOfYear placeholder 'D' found. Possibly DayOfMonth 'd' intended.")::acc
        case _ => acc
      }}
    } else {
      Nil
    }

    patternIssues ++ doubleTimeZoneIssue ++ timeZoneIssue.toSet
  }

  override def verifyStringDateTime(dateTime: String)(implicit parser: EnceladusDateTimeParser): Date = {
    parser.parseDate(dateTime)
  }

}
