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

import org.scalatest.FunSuite
import java.sql.Date
import java.sql.Timestamp
import java.util.TimeZone

case class TestInputRow(id: Int, stringField: String)

class EnceladusDateTimeParserSuite extends FunSuite{
  TimeZoneNormalizer.normalizeJVMTimeZone()

  test("EnceladusDateParser class epoch") {
    val parser: EnceladusDateTimeParser = EnceladusDateTimeParser("epoch")

    val value: String = "1547553153"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-15
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 0 ) //2019-01-15 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class epochmilli") {
    val parser: EnceladusDateTimeParser = EnceladusDateTimeParser("epochmilli")

    val value: String = "1547553153198"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-15
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 198000000 ) //2019-01-15 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class actual pattern without time zone") {
    val parser: EnceladusDateTimeParser = EnceladusDateTimeParser("yyyy_MM_dd:HH.mm.ss")

    val value: String = "2019_01_15:11.52.33"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-15
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 0 ) //2019-01-15 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class actual pattern with standard time zone") {
    val parser: EnceladusDateTimeParser = EnceladusDateTimeParser("yyyy-MM-dd-HH-mm-ss-zz")

    val value: String = "2011-01-31-22-52-33-EST"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(111, 1, 1) //2011-02-01
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(111, 1, 1, 3,52 , 33, 0 ) //2011-02-01 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class actual pattern with offset time zone") {
    val parser: EnceladusDateTimeParser = EnceladusDateTimeParser("yyyy/MM/dd HH:mm:ssXXX")

    val value: String = "1990/01/31 22:52:33+01:00"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(90, 0, 31) //1990-01-31
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(90, 0, 31, 21,52 , 33, 0 ) //1990-01-31 21:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("format") {
    val t = new Timestamp(70, 0, 2, 1, 0, 0, 0) //25 hours to epoch
    val parser1 = EnceladusDateTimeParser("yyyy-MM-dd HH:mm:ss")
    assert(parser1.format(t) == "1970-01-02 01:00:00")
    val parser2 = EnceladusDateTimeParser("epoch")
    assert(parser2.format(t) == "90000")
    val parser3 = EnceladusDateTimeParser("epochmilli")
    assert(parser3.format(t) == "90000000")
  }

}
