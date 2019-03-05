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

package za.co.absa.enceladus.utils.types

import java.security.InvalidParameterException
import org.apache.spark.sql.types.{DateType, DoubleType, TimestampType}
import org.scalatest.FunSuite

class FormatSuite extends FunSuite {

  test("Format class for timestamp") {
    val pattern: String = "yyyy~mm~dd_HH.mm.ss"
    val format: Format = new Format(Some(pattern), Some(TimestampType))
    assert(!format.isDefault)
    assert(format.get == pattern)
    assert(format.getOrElse("foo") == pattern)
    assert(!format.isEpoch)
    val expectedMessage = s"'${format.get}' is not an epoch format"
    val caught = intercept[InvalidParameterException] {
      Format.epochFactor(format)
    }
    assert(caught.getMessage == expectedMessage)
  }


  test("Format class for date") {
    val pattern: String = "yyyy~mm~dd_HH.mm.ss"
    val format: Format = new Format(Some(pattern), Some(DateType))
    assert(!format.isDefault)
    assert(format.get == pattern)
    assert(format.getOrElse("fox") == pattern)
    assert(!format.isEpoch)
    val expectedMessage = s"'${format.get}' is not an epoch format"
    val caught = intercept[InvalidParameterException] {
      Format.epochFactor(format)
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("Format class with default value - timestamp") {
    val format: Format = new Format(None, Some(TimestampType))
    assert(format.isDefault)
    assert(format.get == "yyyy-MM-dd HH:mm:ss")
    assert(format.getOrElse("foo") == "foo")
    assert(!format.isEpoch)
    val expectedMessage = s"'${format.get}' is not an epoch format"
    val caught = intercept[InvalidParameterException] {
      Format.epochFactor(format)
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("Format class with default value - date") {
    val format: Format = new Format(None, Some(DateType))
    assert(format.isDefault)
    assert(format.get == "yyyy-MM-dd")
    assert(format.getOrElse("fox") == "fox")
    assert(!format.isEpoch)
    val expectedMessage = s"'${format.get}' is not an epoch format"
    val caught = intercept[InvalidParameterException] {
      Format.epochFactor(format)
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("Format class with default value - double") {
    val dt: DoubleType = DoubleType
    val expectedMessage = s"No default format defined for data type ${dt.typeName}"
    val caught = intercept[IllegalStateException] {
      new Format(None, Some(dt))
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("Format class with Nones") {
    intercept[NoSuchElementException] {
      new Format(None, None)
    }
  }

  test("Format.isEpoch returns expected values.") {
    val result1 = Format.isEpoch("epoch")
    assert(result1 == true)
    val result2 = Format.isEpoch("milliepoch")
    assert(result2 == true)
    val result3 = Format.isEpoch(" epoch ")
    assert(result3 == false)
    val result4 = Format.isEpoch("add 54")
    assert(result4 == false)
    val result5 = Format.isEpoch("")
    assert(result5 == false)
  }

  test("Format.epochFactor returns expected values.") {
    val result1 = Format.epochFactor("Epoch")
    assert(result1 == 1L)
    val result1000 = Format.epochFactor("mIlLiEpOcH")
    assert(result1000 == 1000L)
    val formatString = "xxxx"
    val expectedMessage = s"'$formatString' is not an epoch format"
    val caught = intercept[InvalidParameterException] {
      Format.epochFactor(formatString)
    }
    assert(caught.getMessage == expectedMessage)
  }
}
