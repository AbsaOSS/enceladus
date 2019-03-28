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

import java.security.InvalidParameterException
import org.apache.spark.sql.types.{DataType, StructField}
import za.co.absa.enceladus.utils.types.TypePattern

/**
  * Class to carry enhanced information about formatting patterns in conversion from/to string
  * @param pattern  actual pattern to format the type conversion; if none global default pattern for the type is used
  * @param forType  type the format is intended for
  * @param assignedDefaultTimeZone default time zone string; should be taken into account only if pattern does not
  *                                contain time zone designation
  */
class DateTimePattern(pattern: Option[String],
                      forType: Option[DataType] = None,
                      assignedDefaultTimeZone: Option[String] = None) extends TypePattern(pattern, forType){

  def isEpoch: Boolean = {
    DateTimePattern.isEpoch(get)
  }

  def epochFactor: Long = {
    DateTimePattern.epochFactor(get)
  }

  def epochMilliFactor: Long = {
    DateTimePattern.epochMilliFactor(get)
  }

  lazy val defaultTimeZone: Option[String] = assignedDefaultTimeZone.filterNot(_ => timeZoneInPattern)
  lazy val timeZoneInPattern: Boolean = DateTimePattern.timeZoneInPattern(get)
  lazy val isTimeZoned: Boolean = timeZoneInPattern || assignedDefaultTimeZone.nonEmpty
}

object DateTimePattern {
  implicit def patternToString(pattern: DateTimePattern): String = pattern.get

  private val epochUnitFactor = 1
  private val epochThousandFactor = 1000

  def apply(structField: StructField ): DateTimePattern = {
    TypePattern(structField).asInstanceOf[DateTimePattern]
  }

  def apply(pattern: String,
            forType: Option[DataType] = None,
            assignedDefaultTimeZone: Option[String] = None): DateTimePattern = {
    new DateTimePattern(Some(pattern), forType, assignedDefaultTimeZone)
  }

  def apply(pattern: String, assignedDefaultTimeZone: String): DateTimePattern = {
    new DateTimePattern(Some(pattern), None, Some(assignedDefaultTimeZone))
  }

  def isEpoch(pattern: String): Boolean = {
    pattern.toLowerCase match {
      case "epoch" | "epochmilli" => true
      case _ => false
    }
  }

  def epochFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case "epoch"      => epochUnitFactor
      case "epochmilli" => epochThousandFactor
      case _            => throw new InvalidParameterException(s"'$pattern' is not an epoch pattern")
    }
  }

  def epochMilliFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case "epoch"      => epochThousandFactor
      case "epochmilli" => epochUnitFactor
      case _            => throw new InvalidParameterException(s"'$pattern' is not an epoch pattern")
    }
  }


  def timeZoneInPattern(pattern: String): Boolean = {
    if (isEpoch(pattern)) {
      true
    } else {
      val sanitized = "(^|[^\\\\])'.*[^\\\\]'".r.replaceAllIn(pattern, "$1") //clear literals
      "[XzZ]".r.findFirstIn(sanitized).nonEmpty
    }
  }}
