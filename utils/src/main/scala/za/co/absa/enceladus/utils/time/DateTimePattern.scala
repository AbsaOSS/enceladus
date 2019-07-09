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

import org.apache.spark.sql.types.{DateType, StructField, TimestampType}
import za.co.absa.enceladus.utils.types.TypePattern
import DateTimePattern.Section

/**
  * Class to carry enhanced information about date/time formatting pattern in conversion from/to string
  * @param pattern  actual pattern to format the type conversion
  * @param isDefault  marks if the pattern is actually an assigned value or taken for global defaults
  */
abstract sealed class DateTimePattern(pattern: String, isDefault: Boolean = false)
  extends TypePattern(pattern, isDefault){

  val isEpoch: Boolean
  val epochFactor: Long
  val epochMilliFactor: Long

  val timeZoneInPattern: Boolean
  val defaultTimeZone: Option[String]
  val isTimeZoned: Boolean
  val millisecondsPosition: Option[Section]
  val microsecondsPosition: Option[Section]
  val nanosecondsPosition: Option[Section]
  val containsSecondFraction: Boolean
  val patternWithoutSecondFractions: String

  override def toString: String = {
    val q = "\""
    s"pattern: $q$pattern$q" + defaultTimeZone.map(x => s" (default time zone: $q$x$q)").getOrElse("")
  }

}

object DateTimePattern {
  import za.co.absa.enceladus.utils.implicits.StringImplicits.StringEnhancements
  import za.co.absa.enceladus.utils.implicits.StructFieldImplicits.{StructFieldEnhancements, PatternInfo}

  type Section = (Int, Int)
  val EpochKeyword = "epoch"
  val EpochMilliKeyword = "epochmilli"

  private val epochUnitFactor = 1
  private val epochThousandFactor = 1000
  private val patternTimeZoneChars = Set('X','z','Z')
  private val patternMilliSecondChar = 'S'
  private val patternMicroSecondChar = 'i'
  private val patternNanoSecondChat = 'n'

  private final case class EpochDTPattern(override val pattern: String,
                                          override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = true
    override val epochFactor: Long = DateTimePattern.epochFactor(pattern)
    override val epochMilliFactor: Long = DateTimePattern.epochMilliFactor(pattern)

    override val timeZoneInPattern: Boolean = true
    override val defaultTimeZone: Option[String] = None
    override val isTimeZoned: Boolean = true
    override val containsSecondFraction: Boolean = pattern != EpochKeyword //not really used here, but for consistency
    override val patternWithoutSecondFractions: String = EpochKeyword
  }

  private final case class StandardDTPattern(override val pattern: String,
                                             assignedDefaultTimeZone: Option[String] = None,
                                             override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = false
    override val epochFactor: Long = 0
    override val epochMilliFactor: Long = 0

    override val timeZoneInPattern: Boolean = DateTimePattern.timeZoneInPattern(pattern)
    override val defaultTimeZone: Option[String] = assignedDefaultTimeZone.filterNot(_ => timeZoneInPattern)
    override val isTimeZoned: Boolean = timeZoneInPattern || defaultTimeZone.nonEmpty
    override val millisecondsPosition: Option[Section] = scanForPlaceholder(pattern, patternMilliSecondChar)
    override val microsecondsPosition: Option[Section] = scanForPlaceholder(pattern, patternMicroSecondChar)
    override val nanosecondsPosition: Option[Section] = scanForPlaceholder(pattern, patternNanoSecondChat)
    override val containsSecondFraction: Boolean = microsecondsPosition.nonEmpty || millisecondsPosition.nonEmpty || nanosecondsPosition.nonEmpty
  }

  private def findSectionEnd(pattern: String, placeHolder: Char, fromIndex: Int): Int = {
    var res = fromIndex
    while ((res < pattern.length) && (pattern(res) == placeHolder)) {
      res += 1
    }
    res - 1
  }

  private def scanForPlaceholder(pattern: String, placeHolder: Char): Option[Section] = {
    val start = pattern.findFirstUnquoted(Set(placeHolder), Set('''))
    start.map(index => (index, findSectionEnd(pattern, placeHolder, index)))
  }

  private def create(pattern: String, assignedDefaultTimeZone: Option[String], isDefault: Boolean): DateTimePattern = {
    if (isEpoch(pattern)) {
      EpochDTPattern(pattern, isDefault)
    } else {
      StandardDTPattern(pattern, assignedDefaultTimeZone, isDefault)
    }
  }

  def apply(pattern: String,
            assignedDefaultTimeZone: Option[String] = None): DateTimePattern = {
    create(pattern, assignedDefaultTimeZone, isDefault = false)
  }

  def fromStructField(structField: StructField ): DateTimePattern = {
    if (!Seq(DateType, TimestampType).contains(structField.dataType)) {
      val typeName = structField.dataType.typeName
      throw new InvalidParameterException(
        s"StructField data type for DateTimePattern has to be DateType or TimestampType, instead $typeName was given."
      )
    }
    val PatternInfo(pattern, isDefault) = structField.readPatternInfo()
    val timeZoneOpt = structField.getMetadataString("timezone")
    create(pattern, timeZoneOpt, isDefault)
  }

  def isEpoch(pattern: String): Boolean = {
    pattern.toLowerCase match {
      case EpochKeyword | EpochMilliKeyword => true
      case _ => false
    }
  }

  def epochFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case EpochKeyword      => epochUnitFactor
      case EpochMilliKeyword => epochThousandFactor
      case _                 => 0
    }
  }

  def epochMilliFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case EpochKeyword      => epochThousandFactor
      case EpochMilliKeyword => epochUnitFactor
      case _                 => 0
    }
  }

  def timeZoneInPattern(pattern: String): Boolean = {
    isEpoch(pattern) || pattern.hasUnquoted(patternTimeZoneChars, Set('''))
  }
}
