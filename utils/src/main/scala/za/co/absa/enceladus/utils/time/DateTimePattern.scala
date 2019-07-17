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
import za.co.absa.enceladus.utils.general.Section
import za.co.absa.enceladus.utils.types.TypePattern

/**
  * Class to carry enhanced information about date/time formatting pattern in conversion from/to string
  * @param pattern  actual pattern to format the type conversion
  * @param isDefault  marks if the pattern is actually an assigned value or taken for global defaults
  */
abstract sealed class DateTimePattern(pattern: String, isDefault: Boolean = false)
  extends TypePattern(pattern, isDefault){

  val isEpoch: Boolean
  val epochFactor: Long

  val timeZoneInPattern: Boolean
  val defaultTimeZone: Option[String]
  val isTimeZoned: Boolean

  val millisecondsPosition: Option[Section]
  val microsecondsPosition: Option[Section]
  val nanosecondsPosition: Option[Section]

  val secondFractionsSections: Seq[Section]
  val patternWithoutSecondFractions: String
  def containsSecondFractions: Boolean = secondFractionsSections.nonEmpty

  override def toString: String = {
    val q = "\""
    s"pattern: $q$pattern$q" + defaultTimeZone.map(x => s" (default time zone: $q$x$q)").getOrElse("")
  }

}

object DateTimePattern {
  import za.co.absa.enceladus.utils.implicits.StringImplicits.StringEnhancements
  import za.co.absa.enceladus.utils.implicits.StructFieldImplicits.{StructFieldEnhancements, PatternInfo}

  val EpochKeyword = "epoch"
  val EpochMilliKeyword = "epochmilli"
  val EpochMicroKeyword = "epochmicro"
  val EpochNanoKeyword = "epochnano"

  private val epochUnitFactor = 1
  private val epoch1kFactor = 1000
  private val epoch1MFactor = 1000000
  private val epoch1GFactor = 1000000000

  private val patternTimeZoneChars = Set('X','z','Z')

  private val patternMilliSecondChar = 'S'
  private val patternMicroSecondChar = 'i'
  private val patternNanoSecondChat = 'n'

  // scalastyle:off magic.number
  private val last3Chars = Section(-3, 3)
  private val last6Chars = Section(-6, 6)
  private val last9Chars = Section(-9, 9)
  private val trio6Back  = Section(-6, 3)
  private val trio9Back  = Section(-9, 3)
  // scalastyle:on magic.number

  private final case class EpochDTPattern(override val pattern: String,
                                          override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = true
    override val epochFactor: Long = DateTimePattern.epochFactor(pattern)

    override val timeZoneInPattern: Boolean = true
    override val defaultTimeZone: Option[String] = None
    override val isTimeZoned: Boolean = true

    override val millisecondsPosition: Option[Section] = pattern match {
      case EpochMilliKeyword => Option(last3Chars)
      case EpochMicroKeyword => Option(trio6Back)
      case EpochNanoKeyword  => Option(trio9Back)
      case _                 => None
    }
    override val microsecondsPosition: Option[Section] = pattern match {
      case EpochMicroKeyword => Option(last3Chars)
      case EpochNanoKeyword  => Option(trio6Back)
      case _                 => None
    }
    override val nanosecondsPosition: Option[Section] = pattern match {
      case EpochNanoKeyword => Option(last3Chars)
      case _                => None
    }
    override val secondFractionsSections: Seq[Section] = pattern match {
      case EpochMilliKeyword => Seq(last3Chars)
      case EpochMicroKeyword => Seq(last6Chars)
      case EpochNanoKeyword  => Seq(last9Chars)
      case _                 => Seq.empty
    }
    override val patternWithoutSecondFractions: String = EpochKeyword
  }

  private final case class StandardDTPattern(override val pattern: String,
                                             assignedDefaultTimeZone: Option[String] = None,
                                             override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = false
    override val epochFactor: Long = 0

    override val timeZoneInPattern: Boolean = DateTimePattern.timeZoneInPattern(pattern)
    override val defaultTimeZone: Option[String] = assignedDefaultTimeZone.filterNot(_ => timeZoneInPattern)
    override val isTimeZoned: Boolean = timeZoneInPattern || defaultTimeZone.nonEmpty

    val (millisecondsPosition, microsecondsPosition, nanosecondsPosition) = analyzeSecondFractionsPositions(pattern)
    override val secondFractionsSections: Seq[Section] = Section.mergeSections(Seq(millisecondsPosition, microsecondsPosition, nanosecondsPosition).flatten)
    override val patternWithoutSecondFractions: String = Section.removeMultiple(pattern, secondFractionsSections)

    private def scanForPlaceholder(withinString: String, placeHolder: Char): Option[Section] = {
      val start = withinString.findFirstUnquoted(Set(placeHolder), Set('''))
      start.map(index => Section.ofSameChars(withinString, index))
    }

    private def analyzeSecondFractionsPositions(withinString: String): (Option[Section], Option[Section], Option[Section]) = {
      val clearedPattern = withinString

      // TODO as part of #677 fix
      val milliSP = scanForPlaceholder(clearedPattern, patternMilliSecondChar)
      val microSP = scanForPlaceholder(clearedPattern, patternMicroSecondChar)
      val nanoSP = scanForPlaceholder(clearedPattern, patternNanoSecondChat)
      (milliSP, microSP, nanoSP)
    }
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
      case EpochKeyword | EpochMilliKeyword | EpochMicroKeyword | EpochNanoKeyword => true
      case _ => false
    }
  }

  def epochFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case EpochKeyword      => epochUnitFactor
      case EpochMilliKeyword => epoch1kFactor
      case EpochMicroKeyword => epoch1MFactor
      case EpochNanoKeyword  => epoch1GFactor
      case _                 => 0
    }
  }

  def timeZoneInPattern(pattern: String): Boolean = {
    isEpoch(pattern) || pattern.hasUnquoted(patternTimeZoneChars, Set('''))
  }
}
