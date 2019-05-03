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

package za.co.absa.enceladus.utils.time

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

/**
  * Enables to parse string to date and timestamp based on the provided format
  * Unlike SimpleDateFormat it also supports keywords to format epoch related values
  * @param pattern  the formatting string, in case it's an epoch format the values wil need to be convertible to Long
  */
case class EnceladusDateTimeParser(pattern: DateTimePattern) {
  private val formatter: Option[SimpleDateFormat] = if (pattern.isEpoch) {
    None
  } else {
    Some(new SimpleDateFormat(pattern, Locale.US))
  }

  private def convertValue(value: String): Long = {
    formatter.map(_.parse(value).getTime).getOrElse(
      value.toLong * pattern.epochMilliFactor
    )
  }

  def parseDate(dateValue: String): Date = {
    new Date(convertValue(dateValue))
  }

  def parseTimestamp(timestampValue: String): Timestamp = {
    new Timestamp(convertValue(timestampValue))
  }

  def format(time: java.util.Date): String = {
    formatter.map(_.format(time)).getOrElse(
      (time.getTime / pattern.epochMilliFactor).toString
    )
  }
}

object EnceladusDateTimeParser {
  def apply(pattern: String): EnceladusDateTimeParser = new EnceladusDateTimeParser(DateTimePattern(pattern))
}
