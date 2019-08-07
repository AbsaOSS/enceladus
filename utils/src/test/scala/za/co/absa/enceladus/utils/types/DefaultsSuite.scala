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

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.util.Success

class DefaultsSuite extends FunSuite {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  test("ByteType") {
    assert(Defaults.getGlobalDefaultWithNull(ByteType, nullable = false) === Success(Some(0.toByte)))
  }

  test("ShortType") {
    assert(Defaults.getGlobalDefaultWithNull(ShortType, nullable = false) === Success(Some(0.toShort)))
  }

  test("IntegerType") {
    assert(Defaults.getGlobalDefaultWithNull(IntegerType, nullable = false) === Success(Some(0)))
  }

  test("LongType") {
    assert(Defaults.getGlobalDefaultWithNull(LongType, nullable = false) === Success(Some(0L)))
  }

  test("FloatType") {
    assert(Defaults.getGlobalDefaultWithNull(FloatType, nullable = false) === Success(Some(0F)))
  }

  test("DoubleType") {
    assert(Defaults.getGlobalDefaultWithNull(DoubleType, nullable = false) === Success(Some(0D)))
  }

  test("StringType") {
    assert(Defaults.getGlobalDefaultWithNull(StringType, nullable = false) === Success(Some("")))
  }

  test("DateType") {
    assert(Defaults.getGlobalDefaultWithNull(DateType, nullable = false) === Success(Some(new Date(0))))
  }

  test("TimestampType") {
    assert(Defaults.getGlobalDefaultWithNull(TimestampType, nullable = false) === Success(Some(new Timestamp(0))))
  }

  test("BooleanType") {
    assert(Defaults.getGlobalDefaultWithNull(BooleanType, nullable = false) === (Success(Some(false))))
  }

  test("DecimalType") {
    assert(Defaults.getGlobalDefaultWithNull(DecimalType(6, 3), nullable = false) === Success(Some(new java.math.BigDecimal("000.000"))))
  }

  test("ArrayType") {
    val dataType = ArrayType(StringType)
    val result = Defaults.getGlobalDefaultWithNull(dataType, nullable = false)
    val e = intercept[IllegalStateException] {
      result.get
    }
    assert(e.getMessage == s"No default value defined for data type ${dataType.typeName}")
  }

  test("Nullable default is None") {
    assert(Defaults.getGlobalDefaultWithNull(BooleanType, nullable = true) === Success(None))
  }
}

