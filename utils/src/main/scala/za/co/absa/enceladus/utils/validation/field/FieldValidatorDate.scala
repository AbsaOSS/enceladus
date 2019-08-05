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
      val placeholders = Set('y', 'M', 'd', 'H', 'm', 's', 'D', 'S', 'i', 'n', 'a', 'k', 'K', 'h')
      val patternChars: Map[Char, Int] = pattern.pattern.countUnquoted(placeholders, Set('''))
      patternChars.foldLeft(List.empty[ValidationIssue]) {(acc, item) => (item._1, item._2 > 0) match {
        case ('y', false) => ValidationWarning("No year placeholder 'yyyy' found.")::acc
        case ('M', false) => ValidationWarning("No month placeholder 'MM' found.")::acc
        case ('d', false) => ValidationWarning("No day placeholder 'dd' found.")::acc
        case ('H', true) => ValidationWarning("Redundant hour placeholder 'H' found.")::acc
        case ('m', true) => ValidationWarning("Redundant minute placeholder 'm' found.")::acc
        case ('s', true) => ValidationWarning("Redundant second placeholder 's' found.")::acc
        case ('S', true) => ValidationWarning("Redundant millisecond placeholder 'S' found.")::acc
        case ('i', true) => ValidationWarning("Redundant microsecond placeholder 'i' found.")::acc
        case ('n', true) => ValidationWarning("Redundant nanosecond placeholder 'n' found.")::acc
        case ('a', true) => ValidationWarning("Redundant am/pm placeholder 'a' found.")::acc
        case ('k', true) => ValidationWarning("Redundant hour placeholder 'k' found.")::acc
        case ('h', true) => ValidationWarning("Redundant hour placeholder 'h' found.")::acc
        case ('K', true) => ValidationWarning("Redundant hour placeholder 'H' found.")::acc
        case ('D', true) if patternChars('d') == 0  =>
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
