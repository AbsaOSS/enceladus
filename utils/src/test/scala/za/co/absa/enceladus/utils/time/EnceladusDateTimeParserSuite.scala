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
import java.text.{ParseException, SimpleDateFormat}

case class TestInputRow(id: Int, stringField: String)

class EnceladusDateTimeParserSuite extends FunSuite{
  TimeZoneNormalizer.normalizeJVMTimeZone()

  test("EnceladusDateParser class epoch") {
    val parser = EnceladusDateTimeParser("epoch")

    val value: String = "1547553153"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class epochmilli") {
    val parser = EnceladusDateTimeParser("epochmilli")

    val value: String = "1547553153198"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33.198")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class epochmicro") {
    val parser = EnceladusDateTimeParser("epochmicro")

    val value: String = "1547553153198765"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33.198765")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class epochnano") {
    val parser = EnceladusDateTimeParser("epochnano")

    val value: String = "1547553153198765432"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33.198765432")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern without time zone") {
    val parser = EnceladusDateTimeParser("yyyy_MM_dd:HH.mm.ss")

    val value: String = "2019_01_15:11.52.33"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern with standard time zone") {
    val parser = EnceladusDateTimeParser("yyyy-MM-dd-HH-mm-ss-zz")

    val value: String = "2011-01-31-22-52-33-EST"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2011-02-01")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2011-02-01 03:52:33")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern with offset time zone") {
    val parser = EnceladusDateTimeParser("yyyy/MM/dd HH:mm:ssXXX")

    val value: String = "1990/01/31 22:52:33+01:00"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("1990-01-31")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("1990-01-31 21:52:33")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern without time zone with milliseconds") {
    val parser = EnceladusDateTimeParser("SSS|yyyy_MM_dd:HH.mm.ss")

    val value: String = "123|2019_01_15:11.52.33"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33.123")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern without time zone and microseconds") {
    val parser = EnceladusDateTimeParser("yyyy_MM_dd:HH.mm.ss.iiiiii")

    val value: String = "2019_01_15:11.52.33.123456"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2019-01-15")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2019-01-15 11:52:33.123456")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern with standard time zone and nanoseconds") {
    val parser = EnceladusDateTimeParser("yyyy-MM-dd-HH-mm-ss.nnnnnnnnn-zz")

    val value: String = "2011-01-31-22-52-33.123456789-EST"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("2011-02-01")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("2011-02-01 03:52:33.123456789")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("EnceladusDateParser class actual pattern with offset time zone and all second fractions") {
    val parser = EnceladusDateTimeParser("nnnSSSyyyy/MM/dd iii HH:mm:ssXXX")

    val value: String = "1234561990/01/31 789 22:52:33+01:00"
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = Date.valueOf("1990-01-31")
    assert(resultDate == expectedDate)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = Timestamp.valueOf("1990-01-31 21:52:33.456789123")
    assert(resultTimestamp == expectedTimestamp)
  }

  test("format") {
    val t: Timestamp = Timestamp.valueOf("1970-01-02 01:00:00.123456789") //25 hours to epoch with some second fractions
    val parser1 = EnceladusDateTimeParser("yyyy-MM-dd HH:mm:ss")
    assert(parser1.format(t) == "1970-01-02 01:00:00")
    val parser2 = EnceladusDateTimeParser("epoch")
    assert(parser2.format(t) == "90000")
    val parser3 = EnceladusDateTimeParser("epochmilli")
    assert(parser3.format(t) == "90000123")
    val parser4 = EnceladusDateTimeParser("epochmicro")
    assert(parser4.format(t) == "90000123456")
    val parser5 = EnceladusDateTimeParser("epochnano")
    assert(parser5.format(t) == "90000123456789")
    val parser6 = EnceladusDateTimeParser("yyyy-MM-dd HH:mm:ss.iiiiii")
    assert(parser6.format(t) == "1970-01-02 01:00:00.123456")
    val parser7 = EnceladusDateTimeParser("(nnn) yyyy-MM-dd (SSS) HH:mm:ss (iii)")
    assert(parser7.format(t) == "(789) 1970-01-02 (123) 01:00:00 (456)")
  }

  test("Lenient interpretation is not accepted") {
    //first lenient interpretation
    val pattern = "dd-MM-yyyy"
    val dateString = "2015-01-01"
    val sdf = new SimpleDateFormat(pattern)
    sdf.parse(dateString)
    //non lenient within EnceladusDateTimeParser
    val parser = EnceladusDateTimeParser(pattern)
    intercept[ParseException] {
      parser.parseDate(dateString)
    }
  }
}
