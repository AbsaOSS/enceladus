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

package za.co.absa.enceladus.utils.validation.field

import org.apache.spark.sql.types.{MetadataBuilder, StructField, TimestampType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue, ValidationWarning}

class FieldValidatorTimestampSuite extends FunSuite  {
  TimeZoneNormalizer.activate()

  private def field(pattern: String, defaultValue: Option[String] = None, defaultTimeZone: Option[String] = None): StructField = {
    val builder = new MetadataBuilder().putString("pattern",pattern)
    val builder2 = defaultValue.map(builder.putString("default", _)).getOrElse(builder)
    val builder3 = defaultTimeZone.map(builder2.putString("timezone", _)).getOrElse(builder2)
    StructField("test_field", TimestampType,  nullable = false, builder3.build())
  }

  test("epoch pattern") {
    assert(FieldValidatorTimestamp.validateStructField(field("epoch")).isEmpty)
    //with default
    assert(FieldValidatorTimestamp.validateStructField(field("epoch", Option("5545556"))).isEmpty)
  }

  test("epochmilli pattern") {
    assert(FieldValidatorTimestamp.validateStructField(field("epochmilli")).isEmpty)
    //with default
    assert(FieldValidatorTimestamp.validateStructField(field("epochmilli", Option("55455560000"))).isEmpty)
  }

  test("timestamp pattern") {
    //no default
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy-MM-dd HH:mm:ss")).isEmpty)
    //default as timestamp
    assert(FieldValidatorTimestamp.validateStructField(field("HH-mm-ss~~dd.MM.yyyy", Option("23-10-11~~31.12.2004"))).isEmpty)
    //extra chars in default
    assert(FieldValidatorTimestamp.validateStructField(field("HH-mm-ss~~dd.MM.yyyy", Option("23-10-11~~31.12.2004kkkkk"))).isEmpty)
  }

  test("timestamp with time zone in pattern") {
    //no default
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy-MM-dd HH:mm:ss zz")).isEmpty)
    //default as timestamp
    assert(FieldValidatorTimestamp.validateStructField(field("HH-mm-ss~~dd.MM.yyyy+zz", Option("23-10-11~~31.12.2004+CET"))).isEmpty)
    //extra chars in default
    assert(FieldValidatorTimestamp.validateStructField(field("yyMMdd_HHmmss_zz", Option("190301_194533_EST!!!!"))).isEmpty)
    //timestamp with offset time zone
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/dd HH:mm:ssXXX", Option("2019/01/31 23:59:59-11:00"))).isEmpty)
  }

  test("invalid pattern") {
    val expected1 = Set(
      ValidationError("Illegal pattern character 'f'"),
      ValidationWarning("No year placeholder 'yyyy' found."),
      ValidationWarning("No month placeholder 'MM' found."),
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("No hour placeholder 'HH' found."),
      ValidationWarning("No minute placeholder 'mm' found."),
      ValidationWarning("No second placeholder 'ss' found.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("fubar")).toSet == expected1)
    val expected2 = Set(
      ValidationError("Illegal pattern character 'x'"),
      ValidationWarning("Placeholder for hour 1-12 'h' found, but no am/pm 'a' placeholder. Possibly 0-23 'H' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyMMdd_hhmmss_zz_xx")).toSet == expected2)
  }

  test("invalid default") {
    //empty default
    val expected1 = Set(
      ValidationError("""Unparseable date: """""),
      ValidationWarning("Placeholder for hour 1-12 'h' found, but no am/pm 'a' placeholder. Possibly 0-23 'H' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyMMdd_hhmmss_zz", Option(""))).toSet == expected1)
    //wrong default
    val expected2 = Set(
      ValidationError("""Unparseable date: "1999-12-31""""),
      ValidationWarning("No hour placeholder 'HH' found."),
      ValidationWarning("No minute placeholder 'mm' found."),
      ValidationWarning("No second placeholder 'ss' found.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/dd", Option("1999-12-31"))).toSet == expected2)
    //invalid epoch default
    val expected3 = Set(
      ValidationError("""For input string: "2019-01-01"""")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("epoch", Option("2019-01-01"))).toSet == expected3)
    //timestamp pattern, date default
    val expected4 = Set(
      ValidationError("""Unparseable date: "31.12.2004""""),
      ValidationWarning("Placeholder for hour 1-12 'h' found, but no am/pm 'a' placeholder. Possibly 0-23 'H' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("dd.MM.yyyy hh-mm-ss", Option("31.12.2004"))).toSet == expected4)
    //epoch overflow
    val expected5 = Set(
      ValidationError("""For input string: "8748743743948390823948239084294938231122123"""")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("epoch", Option("8748743743948390823948239084294938231122123"))).toSet == expected5)
  }

  test("utilizing default time zone") {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val value = Option("2000-01-01 00:00:00")
    // full name
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option("Africa/Johannesburg"))).isEmpty)
    // abbreviation
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option("CET"))).isEmpty)
    // offset to GMT
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option("Etc/GMT-6"))).isEmpty)
  }

  test("issues with default time zone") {
    def expected(timeZone: String): Set[ValidationIssue] = {
      val q ="\""
      Set(ValidationError(s"$q$timeZone$q is not a valid time zone designation"))
    }
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val value = Option("2000-01-01 00:00:00")
    // offset
    val tz1 = "-03:00"
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option(tz1))).toSet == expected(tz1))
    // empty
    val tz2 = ""
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option(tz2))).toSet == expected(tz2))
    // gibberish
    val tz3 = "Gjh878-++_?"
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option(tz3))).toSet == expected(tz3))
    // non-existing
    val tz4 = "Africa/New York"
    assert(FieldValidatorTimestamp.validateStructField(field(pattern, value, Option(tz4))).toSet == expected(tz4))
  }

  test("warning issues: double time zone") {
    val expected = Set(
      ValidationWarning("Pattern includes time zone placeholder and default time zone is also defined (will never be used)")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy-MM-dd HH:mm:ss XX", None, Option("CET"))).toSet == expected)
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy-MM-dd HH:mm:ss zz", None, Option("CET"))).toSet == expected)
  }

  test("warning issues: missing placeholders") {
    val expected = Set(
      ValidationWarning("No year placeholder 'yyyy' found."),
      ValidationWarning("No month placeholder 'MM' found."),
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("No hour placeholder 'HH' found."),
      ValidationWarning("No minute placeholder 'mm' found."),
      ValidationWarning("No second placeholder 'ss' found.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("GG")).toSet == expected)
  }

  test("warning issues: missing placeholders with default time zone") {
    val expected = Set(
      ValidationWarning("No year placeholder 'yyyy' found."),
      ValidationWarning("No month placeholder 'MM' found."),
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("No hour placeholder 'HH' found."),
      ValidationWarning("No minute placeholder 'mm' found."),
      ValidationWarning("No second placeholder 'ss' found.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("GG", None, Option("CET"))).toSet == expected)
  }

  test("warning issues: day placeholder wrong case") {
    val expected = Set(
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("Rarely used DayOfYear placeholder 'D' found. Possibly DayOfMonth 'd' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/DD HH-mm-ss")).toSet == expected)
  }

  test("warning issues: h instead of H") {
    val expected = Set(
      ValidationWarning("Placeholder for hour 1-12 'h' found, but no am/pm 'a' placeholder. Possibly 0-23 'H' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/dd hh-mm-ss")).toSet == expected)
  }

  test("warning issues: K instead of k") {
    val expected = Set(
      ValidationWarning("Placeholder for hour 0-11 'K' found, but no am/pm 'a' placeholder. Possibly 1-24 'k' intended.")
    )
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/dd KK-mm-ss")).toSet == expected)
  }

  test("warning issues: k and no H is ok") {
    assert(FieldValidatorTimestamp.validateStructField(field("yyyy/MM/dd kk-mm-ss")).isEmpty)

  }

  test("all placeholders") {
    assert(FieldValidatorTimestamp.validateStructField(field("X GG yyyy MM ww W DDD dd F E a HH kk KK hh mm ss SSS ZZ zzz")).isEmpty)
  }
}
