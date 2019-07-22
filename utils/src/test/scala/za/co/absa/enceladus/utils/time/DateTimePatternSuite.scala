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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.general.Section

class DateTimePatternSuite extends FunSuite {

  test("Pattern for timestamp") {
    val pattern: String = "yyyy~mm~dd_HH.mm.ss"
    val dateTimePattern = DateTimePattern(pattern)
    assert(!dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == pattern)
    assert(!dateTimePattern.isEpoch)
    assert(0 == dateTimePattern.epochFactor)
  }

  test("Pattern for date") {
    val pattern: String = "yyyy~mm~dd"
    val dateTimePattern = DateTimePattern(pattern)
    assert(!dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == pattern)
    assert(!dateTimePattern.isEpoch)
    assert(dateTimePattern.epochFactor == 0)
  }

  test("DateTimePattern.isEpoch should return true for known keywords, regardless of case") {
    val result1 = DateTimePattern.isEpoch("epoch")
    assert(result1)
    val result2 = DateTimePattern.isEpoch("epochmilli")
    assert(result2)
    val result3 = DateTimePattern.isEpoch(" epoch ")
    assert(!result3)
    val result4 = DateTimePattern.isEpoch("add 54")
    assert(!result4)
    val result5 = DateTimePattern.isEpoch("")
    assert(!result5)
    val result6 = DateTimePattern.isEpoch("epochMicro")
    assert(result6)
    val result7 = DateTimePattern.isEpoch("EPOCHNANO")
    assert(result7)
  }

  test("DateTimePattern.epochFactor returns appropriate power of ten corresponding the keyword") {
    var result = DateTimePattern.epochFactor("Epoch")
    assert(result == 1L)
    result = DateTimePattern.epochFactor("EpOcHmIlLi")
    assert(result == 1000L)
    result = DateTimePattern.epochFactor("EpochMICRO")
    assert(result == 1000000L)
    result = DateTimePattern.epochFactor("epochnano")
    assert(result == 1000000000L)
    result = DateTimePattern.epochFactor("zoom")
    assert(result == 0L)
  }

  test("Time zone in epoch pattern") {
    val dateTimePattern1 = DateTimePattern("epoch")
    assert(dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern("epochmilli")
    assert(dateTimePattern2.timeZoneInPattern)
    val dateTimePattern3 = DateTimePattern("epochmicro")
    assert(dateTimePattern3.timeZoneInPattern)
    val dateTimePattern4 = DateTimePattern("epochnano")
    assert(dateTimePattern4.timeZoneInPattern)
  }

  test("Time zone NOT in pattern") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss")
    assert(!dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern("")
    assert(!dateTimePattern2.timeZoneInPattern)
  }

  test("Standard time zone in pattern") {
    val dateTimePattern1 = DateTimePattern("ZZ yyyy-MM-dd HH:mm:ss")
    assert(dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern(" HH:mm:ss ZZZZ yyyy-MM-dd")
    assert(dateTimePattern2.timeZoneInPattern)
  }

  test("Offset time zone in pattern") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ssXX")
    assert(dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern("HH:mm:ss XX yyyy-MM-dd")
    assert(dateTimePattern2.timeZoneInPattern)
    val dateTimePattern3 = DateTimePattern("XXX HH:mm:ss yyyy-MM-dd")
    assert(dateTimePattern3.timeZoneInPattern)
  }

  test("Time zone with literals in the pattern") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss'zz'")
    assert(!dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern("'XXX: 'HH:mm:ss XX yyyy-MM-dd")
    assert(dateTimePattern2.timeZoneInPattern)
    val dateTimePattern3 = DateTimePattern("""'Date:'yyyy-MM-dd HH:mm:ss\'ZZ\'""")
    assert(dateTimePattern3.timeZoneInPattern)
  }

  test("Default time zone - not present") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss")
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd", assignedDefaultTimeZone = None)
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
    val dateTimePattern3 = DateTimePattern("")
    assert(dateTimePattern3.defaultTimeZone.isEmpty)
  }

  test("Default time zone - present") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss", assignedDefaultTimeZone = Some("CET"))
    assert(dateTimePattern1.defaultTimeZone.contains("CET"))
    val dateTimePattern2 = DateTimePattern("", assignedDefaultTimeZone = Some(""))
    assert(dateTimePattern2.defaultTimeZone.contains(""))
  }

  test("Default time zone - overridden by time zone in pattern") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss zz", Some("CST")) //Standard time zone
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd HH:mm:ssXX", Some("WST")) //Offset time zone
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
  }

  test("Default time zone - epoch") {
    val dateTimePattern1 = DateTimePattern("epochmilli", Some("WST"))
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("epoch", Some("CET"))
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
    val dateTimePattern3 = DateTimePattern("epochmicro", Some("WST"))
    assert(dateTimePattern3.defaultTimeZone.isEmpty)
    val dateTimePattern4 = DateTimePattern("epochnano", Some("CET"))
    assert(dateTimePattern4.defaultTimeZone.isEmpty)
  }

  test("Is NOT time-zoned ") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss")
    assert(!dateTimePattern1.isTimeZoned)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd", assignedDefaultTimeZone = None)
    assert(!dateTimePattern2.isTimeZoned)
  }

  test("Is time-zoned - default time zone") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ss", Some("EST"))
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - standard time zone in pattern") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ss zz") //Standard time zone
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - offset time zone in pattern") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ssXX") //Offset time zone
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - epoch") {
    val dateTimePattern = DateTimePattern("epoch")
    assert(dateTimePattern.isTimeZoned)
  }

  test("fromStructField") {
    def check(dtp: DateTimePattern,
              pattern: String,
              isDefault: Boolean,
              timeZoneInPattern: Boolean,
              defaultTimeZone: Option[String]
             ): Unit = {
      assert(dtp.pattern == pattern)
      assert(dtp.isDefault == isDefault)
      assert(!dtp.isEpoch)
      assert(dtp.epochFactor == 0)
      assert(dtp.timeZoneInPattern == timeZoneInPattern)
      assert(dtp.defaultTimeZone == defaultTimeZone)
    }

    //wrong type
    val sfwt = StructField("stringField", StringType)
    val expectedMessage = s"StructField data type for DateTimePattern has to be DateType or TimestampType, instead ${sfwt.dataType.typeName} was given."
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.fromStructField(sfwt)
    }
    assert(caught.getMessage  == expectedMessage)
    //timestamp type - default
    val sftd =StructField("timestampPatternDefaultField", TimestampType, nullable = true)
    val dtptd = DateTimePattern.fromStructField(sftd)
    check(dtptd, "yyyy-MM-dd HH:mm:ss", isDefault = true, timeZoneInPattern = false, None)
    //date type - default
    val sfdd =StructField("datePatternDefaultField", DateType, nullable = true)
    val dtpdd = DateTimePattern.fromStructField(sfdd)
    check(dtpdd, "yyyy-MM-dd", isDefault = true, timeZoneInPattern = false, None)
    //timestamp type - with pattern
    val pattern1 = "yyyy/MM/dd_HHmmss"
    val sf1 = StructField("timestampField", TimestampType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern1).build)
    val dtp1 = DateTimePattern.fromStructField(sf1)
    check(dtp1, pattern1, isDefault = false, timeZoneInPattern = false, None)
    val pattern2 = "yyyy~MM~dd~HH~mm~ss~z"
    val sf2 = StructField("timestampField", TimestampType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern2).build)
    val dtp2 = DateTimePattern.fromStructField(sf2)
    check(dtp2, pattern2, isDefault = false, timeZoneInPattern = true, None)
    //date type - with pattern
    val pattern3 = "dd.MM.yyyy"
    val sf3 = StructField("dateField", DateType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern3).build)
    val dtp3 = DateTimePattern.fromStructField(sf3)
    check(dtp3, pattern3, isDefault = false, timeZoneInPattern = false, None)
    val pattern4 = "dd_MM_yyyy_X"
    val sf4 = StructField("dateField", TimestampType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern4).build)
    val dtp4 = DateTimePattern.fromStructField(sf4)
    check(dtp4, pattern4, isDefault = false, timeZoneInPattern = true, None)
    //timestamp type - with pattern & timezone
    val pattern5 = "yyyy/MM/dd_HHmmss"
    val timeZone5 = "CET"
    val sf5 = StructField("timestampField", TimestampType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern5).putString("timezone", timeZone5).build)
    val dtp5 = DateTimePattern.fromStructField(sf5)
    check(dtp5, pattern5, isDefault = false, timeZoneInPattern = false, Some(timeZone5))
    //date type - with pattern & timezone
    val pattern6 = "dd.MM.yyyy"
    val timeZone6 = "EET"
    val sf6 = StructField("dateField", DateType, nullable = true,
      new MetadataBuilder().putString("pattern", pattern6).putString("timezone", timeZone6).build)
    val dtp6 = DateTimePattern.fromStructField(sf6)
    check(dtp6, pattern6, isDefault = false, timeZoneInPattern = false, Some(timeZone6))
  }

  test("Second fractions detection in epoch") {
    val dtp = DateTimePattern("epoch")
    assert(dtp.millisecondsPosition.isEmpty)
    assert(dtp.microsecondsPosition.isEmpty)
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections.isEmpty)
    assert(dtp.patternWithoutSecondFractions == "epoch")
    assert(!dtp.containsSecondFractions)
  }

  test("Second fractions detection in epochmilli") {
    val dtp = DateTimePattern("epochmilli")
    assert(dtp.millisecondsPosition.contains(Section(-3,3)))
    assert(dtp.microsecondsPosition.isEmpty)
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections == Seq(Section(-3,3)))
    assert(dtp.patternWithoutSecondFractions == "epoch")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in epochmicro") {
    val dtp = DateTimePattern("epochmicro")
    assert(dtp.millisecondsPosition.contains(Section(-6,3)))
    assert(dtp.microsecondsPosition.contains(Section(-3,3)))
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections == Seq(Section(-6,6)))
    assert(dtp.patternWithoutSecondFractions == "epoch")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in epochnano") {
    val dtp = DateTimePattern("epochnano")
    assert(dtp.millisecondsPosition.contains(Section(-9,3)))
    assert(dtp.microsecondsPosition.contains(Section(-6,3)))
    assert(dtp.nanosecondsPosition.contains(Section(-3,3)))
    assert(dtp.secondFractionsSections == Seq(Section(-9,9)))
    assert(dtp.patternWithoutSecondFractions == "epoch")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in regular pattern - milliseconds") {
    val pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    val dtp = DateTimePattern(pattern)
    assert(dtp.millisecondsPosition.contains(Section(20,3)))
    assert(dtp.microsecondsPosition.isEmpty)
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections == Seq(Section(20,3)))
    assert(dtp.patternWithoutSecondFractions == "yyyy-MM-dd HH:mm:ss.")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in regular pattern - microseconds") {
    val pattern = "yyyy-MM-dd HH:mm:ss.iiiiii"
    val dtp = DateTimePattern(pattern)
    assert(dtp.millisecondsPosition.isEmpty)
    assert(dtp.microsecondsPosition.contains(Section(20,6)))
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections == Seq(Section(20,6)))
    assert(dtp.patternWithoutSecondFractions == "yyyy-MM-dd HH:mm:ss.")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in regular pattern - nanoseconds") {
    val pattern = "yyyy-MM-dd HH:mm:ss.nnnnnnnnn"
    val dtp = DateTimePattern(pattern)
    assert(dtp.millisecondsPosition.isEmpty)
    assert(dtp.microsecondsPosition.isEmpty)
    assert(dtp.nanosecondsPosition.contains(Section(20,9)))
    assert(dtp.secondFractionsSections == Seq(Section(20,9)))
    assert(dtp.patternWithoutSecondFractions == "yyyy-MM-dd HH:mm:ss.")
    assert(dtp.containsSecondFractions)
  }

  test("Second fractions detection in regular pattern - milli-, micro-, nanosecond combined") {
    val pattern = "nnniii|yyyy-MM-dd SSS HH:mm:ss"
    val dtp = DateTimePattern(pattern)
    assert(dtp.millisecondsPosition.contains(Section(18,3)))
    assert(dtp.microsecondsPosition.contains(Section(3,3)))
    assert(dtp.nanosecondsPosition.contains(Section(0,3)))
    assert(dtp.secondFractionsSections == Seq(Section(18,3), Section(0, 6)))
    assert(dtp.patternWithoutSecondFractions == "|yyyy-MM-dd  HH:mm:ss")
    assert(dtp.containsSecondFractions)
  }


  test("Second fractions detection in regular pattern - not present") {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dtp = DateTimePattern(pattern)
    assert(dtp.millisecondsPosition.isEmpty)
    assert(dtp.microsecondsPosition.isEmpty)
    assert(dtp.nanosecondsPosition.isEmpty)
    assert(dtp.secondFractionsSections.isEmpty)
    assert(dtp.patternWithoutSecondFractions == "yyyy-MM-dd HH:mm:ss")
    assert(!dtp.containsSecondFractions)
  }
}
