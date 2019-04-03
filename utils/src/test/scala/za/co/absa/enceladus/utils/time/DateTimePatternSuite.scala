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
    assert(dateTimePattern.get == pattern)
    assert(dateTimePattern.getOrElse("foo") == pattern)
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.get}' is not an epoch pattern"
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
    assert(dateTimePattern.get == pattern)
    assert(dateTimePattern.getOrElse("fox") == pattern)
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.get}' is not an epoch pattern"
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
    assert(dateTimePattern.get == "yyyy-MM-dd HH:mm:ss")
    assert(dateTimePattern.getOrElse("foo") == "foo")
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.get}' is not an epoch pattern"
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
    assert(dateTimePattern.get == "yyyy-MM-dd")
    assert(dateTimePattern.getOrElse("fox") == "fox")
    assert(!dateTimePattern.isEpoch)
    val expectedMessage = s"'${dateTimePattern.get}' is not an epoch pattern"
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
}
