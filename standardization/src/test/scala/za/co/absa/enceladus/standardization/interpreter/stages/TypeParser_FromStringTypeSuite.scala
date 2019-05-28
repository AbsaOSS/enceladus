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

package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.spark.sql.types.{ByteType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructField, TimestampType}
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParserSuiteTemplate.Input
import za.co.absa.enceladus.utils.time.DateTimePattern

class TypeParser_FromStringTypeSuite extends TypeParserSuiteTemplate {

  private val input = Input(
    baseType = StringType,
    defaultValueDate = "01.01.1970",
    defaultValueTimestamp = "01.01.1970 00:00:00",
    datePattern = "dd.MM.yyyy",
    timestampPattern = "dd.MM.yyyy HH:mm:ss",
    fixedTimezone = "CET",
    path = "",
    datetimeNeedsPattern = false
  )

  override protected def createCastTemplate(toType: DataType, pattern: String, timezone: Option[String]): String = {
    val isEpoch = DateTimePattern.isEpoch(pattern)
    (toType, isEpoch, timezone) match {
      case (DateType, true, _)          => s"to_date(from_unixtime((CAST(`%s` AS BIGINT) / ${DateTimePattern.epochFactor(pattern)}L), 'yyyy-MM-dd'), 'yyyy-MM-dd')"
      case (TimestampType, true, _)     => s"to_timestamp(from_unixtime((CAST(`%s` AS BIGINT) / ${DateTimePattern.epochFactor(pattern)}L), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')"
      case (DateType, _, Some(tz))      => s"to_date(to_utc_timestamp(to_timestamp(`%s`, '$pattern'), '$tz'))"
      case (TimestampType, _, Some(tz)) => s"to_utc_timestamp(to_timestamp(`%s`, '$pattern'), $tz)"
      case (DateType, _, _)             => s"to_date(`%s`, '$pattern')"
      case (TimestampType, _, _)        => s"to_timestamp(`%s`, '$pattern')"
      case _                            => s"CAST(%s AS ${toType.sql})"
    }
  }

  override protected def createErrorCondition(srcField: String, target: StructField, castS: String): String = {
    target.dataType match {
      case FloatType | DoubleType => s"(($castS IS NULL) OR isnan($castS)) OR ($castS IN (Infinity, -Infinity))"
      case ByteType | ShortType | IntegerType | LongType => s"($castS IS NULL) OR contains($srcField, .)"
      case _ => s"$castS IS NULL"
    }
  }

  test("Within the column - type stays, nullable") {
    doTestWithinColumnNullable(input)
  }

  test("Within the column - type stays, not nullable") {
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

  test("Into timestamp field, no pattern") {
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

  test("Into timestamp field with pattern and default") {
    doTestIntoTimestampFieldWithPatternAndDefault(input)
  }

  test("Into date field with pattern and fixed time zone") {
    doTestIntoDateFieldWithPatternAndTimeZone(input)
  }

  test("Into timestamp field with pattern and fixed time zone") {
    doTestIntoTimestampFieldWithPatternAndTimeZone(input)
  }

  test("Into date field with epoch pattern") {
    doTestIntoDateFieldWithEpochPattern(input)
  }

  test("Into timestamp field with epoch pattern") {
    doTestIntoTimestampFieldWithEpochPattern(input)
  }

}
