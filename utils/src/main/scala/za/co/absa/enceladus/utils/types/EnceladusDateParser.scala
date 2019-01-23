/*
 * Copyright 2018-2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.types

import java.text.SimpleDateFormat
import java.sql.Date
import java.sql.Timestamp

/**
  * Enables to parse string to date and timestamp based on the provided format
  * Unlike SimpleDateFormat it also supports an epoch ("epoch", "milliepoch"...) formatting strings
  * @param pattern  the formatting string, in case it's an epoch format the values wil need to be convertible to Long
  */
case class EnceladusDateParser(pattern: Format) {
  private val simpleDateFormat: Option[SimpleDateFormat] = if (pattern.isEpoch) {
    None
  } else {
    Some(new SimpleDateFormat(pattern))
  }

  private def convertValue(value: String): Long = {
    simpleDateFormat.map(_.parse(value).getTime).getOrElse(value.toLong * (1000 / pattern.epochFactor))
  }

  def parseDate(dateValue: String): Date = {
    new Date(convertValue(dateValue))
  }

  def parseTimestamp(timestampValue: String): Timestamp = {
    new Timestamp(convertValue(timestampValue))
  }
}

object EnceladusDateParser {
  def apply(pattern: String): EnceladusDateParser = new EnceladusDateParser(Format(pattern))
}
