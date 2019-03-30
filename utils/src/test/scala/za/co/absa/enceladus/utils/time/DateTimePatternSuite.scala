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
import org.apache.spark.sql.types.{DateType, DoubleType, TimestampType}
import org.scalatest.FunSuite

class DateTimePatternSuite extends FunSuite {

  test("Format class for timestamp") {
    val pattern: String = "yyyy~mm~dd_HH.mm.ss"
    val dateTimePattern = new DateTimePattern(Some(pattern), Some(TimestampType))
    assert(!dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == pattern)
    assert(dateTimePattern.getOrElse("foo") == pattern)
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.pattern}' is not an epoch pattern"
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.epochFactor(dateTimePattern)
    }
    assert(caught.getMessage == expectedMessage)
    val caught2 = intercept[InvalidParameterException] {
      DateTimePattern.epochMilliFactor(dateTimePattern)
    }
    assert(caught2.getMessage == expectedMessage)
  }


  test("Format class for date") {
    val pattern: String = "yyyy~mm~dd_HH.mm.ss"
    val dateTimePattern = new DateTimePattern(Some(pattern), Some(DateType))
    assert(!dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == pattern)
    assert(dateTimePattern.getOrElse("fox") == pattern)
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.pattern}' is not an epoch pattern"
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.epochFactor(dateTimePattern)
    }
    assert(caught.getMessage == expectedMessage)
    val caught2 = intercept[InvalidParameterException] {
      DateTimePattern.epochMilliFactor(dateTimePattern)
    }
    assert(caught2.getMessage == expectedMessage)
  }

  test("Format class with default value - timestamp") {
    val dateTimePattern = new DateTimePattern(None, Some(TimestampType))
    assert(dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == "yyyy-MM-dd HH:mm:ss")
    assert(dateTimePattern.getOrElse("foo") == "foo")
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.pattern}' is not an epoch pattern"
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.epochFactor(dateTimePattern)
    }
    assert(caught.getMessage == expectedMessage)
    val caught2 = intercept[InvalidParameterException] {
      DateTimePattern.epochMilliFactor(dateTimePattern)
    }
    assert(caught2.getMessage == expectedMessage)
  }

  test("Format class with default value - date") {
    val dateTimePattern = new DateTimePattern(None, Some(DateType))
    assert(dateTimePattern.isDefault)
    assert(dateTimePattern.pattern == "yyyy-MM-dd")
    assert(dateTimePattern.getOrElse("fox") == "fox")
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.pattern}' is not an epoch pattern"
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.epochFactor(dateTimePattern)
    }
    assert(caught.getMessage == expectedMessage)
    val caught2 = intercept[InvalidParameterException] {
      DateTimePattern.epochMilliFactor(dateTimePattern)
    }
    assert(caught2.getMessage == expectedMessage)
  }

  test("Format class with default value - double") {
    val dt = DoubleType
    val expectedMessage = s"No default format defined for data type ${dt.typeName}"
    val caught = intercept[IllegalStateException] {
      new DateTimePattern(None, Some(dt))
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("Format class with Nones") {
    intercept[NoSuchElementException] {
      new DateTimePattern(None, None)
    }
  }

  test("Format.isEpoch returns expected values.") {
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
  }

  test("Format.epochFactor returns expected values.") {
    val result1 = DateTimePattern.epochFactor("Epoch")
    assert(result1 == 1L)
    val result2 = DateTimePattern.epochFactor("EpOcHmIlLi")
    assert(result2 == 1000L)
    val result3 = DateTimePattern.epochMilliFactor("Epoch")
    assert(result3 == 1000L)
    val result4 = DateTimePattern.epochMilliFactor("EpOcHmIlLi")
    assert(result4 == 1L)
    val formatString = "xxxx"
    val expectedMessage = s"'$formatString' is not an epoch pattern"
    val caught = intercept[InvalidParameterException] {
      DateTimePattern.epochFactor(formatString)
    }
    assert(caught.getMessage == expectedMessage)
    val caught2 = intercept[InvalidParameterException] {
      DateTimePattern.epochMilliFactor(formatString)
    }
    assert(caught2.getMessage == expectedMessage)
  }

  test("Epoch in pattern") {
    val dateTimePattern1 = DateTimePattern("epoch")
    assert(dateTimePattern1.timeZoneInPattern)
    val dateTimePattern2 = DateTimePattern("epochmilli")
    assert(dateTimePattern2.timeZoneInPattern)
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
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss", Some(TimestampType))
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd", Some(DateType), assignedDefaultTimeZone = None)
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
    val dateTimePattern3 = DateTimePattern("")
    assert(dateTimePattern3.defaultTimeZone.isEmpty)
  }

  test("Default time zone - present") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss", Some(TimestampType), assignedDefaultTimeZone = Some("CET"))
    assert(dateTimePattern1.defaultTimeZone.contains("CET"))
    val dateTimePattern2 = DateTimePattern("", assignedDefaultTimeZone = "")
    assert(dateTimePattern2.defaultTimeZone.contains(""))
  }

  test("Default time zone - overridden by time zone in pattern") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss zz", Some(TimestampType), Some("CST")) //Standard time zone
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd HH:mm:ssXX", Some(TimestampType), Some("WST")) //Offset time zone
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
  }

  test("Default time zone - epoch") {
    val dateTimePattern1 = DateTimePattern("epochmilli", "WST")
    assert(dateTimePattern1.defaultTimeZone.isEmpty)
    val dateTimePattern2 = DateTimePattern("epoch", "CET")
    assert(dateTimePattern2.defaultTimeZone.isEmpty)
  }

  test("Is NOT time-zoned ") {
    val dateTimePattern1 = DateTimePattern("yyyy-MM-dd HH:mm:ss", Some(TimestampType))
    assert(!dateTimePattern1.isTimeZoned)
    val dateTimePattern2 = DateTimePattern("yyyy-MM-dd", Some(DateType), None)
    assert(!dateTimePattern2.isTimeZoned)
  }

  test("Is time-zoned - default time zone") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ss", Some(TimestampType), Some("EST"))
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - standard time zone in pattern") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ss zz", Some(TimestampType)) //Standard time zone
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - offset time zone in pattern") {
    val dateTimePattern = DateTimePattern("yyyy-MM-dd HH:mm:ssXX", Some(TimestampType)) //Offset time zone
    assert(dateTimePattern.isTimeZoned)
  }

  test("Is time-zoned - epoch") {
    val dateTimePattern = DateTimePattern("epoch")
    assert(dateTimePattern.isTimeZoned)
  }

}
