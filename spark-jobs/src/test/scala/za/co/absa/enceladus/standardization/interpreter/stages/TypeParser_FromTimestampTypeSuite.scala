/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.spark.sql.types._
import TypeParserSuiteTemplate.Input
import za.co.absa.enceladus.utils.time.DateTimePattern

class TypeParser_FromTimestampTypeSuite extends TypeParserSuiteTemplate  {

  private val input = Input(
    baseType = TimestampType,
    defaultValueDate = "2000-01-01",
    defaultValueTimestamp = "2010-12-31 23:59:59",
    datePattern = "yyyy-MM-dd",
    timestampPattern = "yyyy-MM-dd HH:mm:ss",
    fixedTimezone = "CST",
    path = "timestamp",
    datetimeNeedsPattern = false
  )

  override protected def createCastTemplate(toType: DataType, pattern: String, timezone: Option[String]): String = {
    val isEpoch = DateTimePattern.isEpoch(pattern)
    (toType, isEpoch, timezone) match {
      case (DateType, true, _)          => s"to_date(CAST((CAST(%s AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}) AS TIMESTAMP))"
      case (TimestampType, true, _)     => s"CAST((CAST(%s AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}) AS TIMESTAMP)"
      case (TimestampType, _, Some(tz)) => s"to_utc_timestamp(%s, $tz)"
      case (DateType, _, Some(tz))      => s"to_date(to_utc_timestamp(%s, $tz))"
      case (TimestampType, _, _)        => "%s"
      case (DateType, _, _)             => "to_date(%s)"
      case _                            => s"CAST(%s AS ${toType.sql})"
    }
  }

  override protected def createErrorCondition(srcField: String, target: StructField, castS: String): String = {
    target.dataType match {
      case FloatType | DoubleType => s"(($castS IS NULL) OR isnan($castS)) OR ($castS IN (Infinity, -Infinity))"
      case _ => s"$castS IS NULL"
    }
  }

  test("Within the column - type stays, nullable") {
    doTestWithinColumnNullable(input)
  }

  // todo issue #1720
  ignore("Within the column - type stays, not nullable") {
    doTestWithinColumnNotNullable(input)
  }

  test("Into string field") {
    doTestIntoStringField(input)
  }

  test("Into float field") {
    doTestIntoFloatField(input)
  }

  test("Into integer field") {
    doTestIntoIntegerField(input)
  }

  test("Into boolean field") {
    doTestIntoBooleanField(input)
  }

  test("Into date field, no pattern") {
    doTestIntoDateFieldNoPattern(input)
  }

  // todo issue #1720
  ignore("Into timestamp field, no pattern") {
    doTestIntoTimestampFieldNoPattern(input)
  }

  test("Into date field with pattern") {
    doTestIntoDateFieldWithPattern(input)
  }

  test("Into timestamp field with pattern") {
    doTestIntoDateFieldWithPattern(input)
  }

  test("Into date field with pattern and default") {
    doTestIntoDateFieldWithPatternAndDefault(input)
  }

  // todo issue #1720
  ignore("Into timestamp field with pattern and default") {
    doTestIntoTimestampFieldWithPatternAndDefault(input)
  }

  test("Into date field with pattern and fixed time zone") {
    doTestIntoDateFieldWithPatternAndTimeZone(input)
  }

  // todo issue #1720
  ignore("Into timestamp field with pattern and fixed time zone") {
    doTestIntoTimestampFieldWithPatternAndTimeZone(input)
  }

  test("Into date field with epoch pattern") {
    doTestIntoDateFieldWithEpochPattern(input)
  }

  // todo issue #1720
  ignore("Into timestamp field with epoch pattern") {
    doTestIntoTimestampFieldWithEpochPattern(input)
  }

}
