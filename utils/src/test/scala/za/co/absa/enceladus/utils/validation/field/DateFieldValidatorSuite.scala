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

import org.apache.spark.sql.types.{DateType, MetadataBuilder, StructField}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue, ValidationWarning}

class DateFieldValidatorSuite extends FunSuite  {
  TimeZoneNormalizer.normalizeJVMTimeZone()

  private def field(pattern: String, defaultValue: Option[String] = None, defaultTimeZone: Option[String] = None): TypedStructField = {
    val builder = new MetadataBuilder().putString("pattern",pattern)
    val builder2 = defaultValue.map(builder.putString("default", _)).getOrElse(builder)
    val builder3 = defaultTimeZone.map(builder2.putString("timezone", _)).getOrElse(builder2)
    val result = StructField("test_field", DateType,  nullable = false, builder3.build())
    TypedStructField(result)
  }

  test("epoch pattern") {
    assert(DateFieldValidator.validate(field("epoch")).isEmpty)
    //with default
    assert(DateFieldValidator.validate(field("epoch", Option("5545556"))).isEmpty)
  }

  test("epochmilli pattern") {
    assert(DateFieldValidator.validate(field("epochmilli")).isEmpty)
    //with default
    assert(DateFieldValidator.validate(field("epochmilli", Option("5545556000"))).isEmpty)
  }

  test("epochmicro pattern") {
    assert(DateFieldValidator.validate(field("epochmicro")).isEmpty)
    //with default
    assert(DateFieldValidator.validate(field("epochmicro", Option("5545556000111"))).isEmpty)
  }

  test("epochnano pattern") {
    assert(DateFieldValidator.validate(field("epochnano")).isEmpty)
    //with default
    assert(DateFieldValidator.validate(field("epochnano", Option("5545556000111222"))).isEmpty)
  }

  test("date pattern") {
    //no default
    assert(DateFieldValidator.validate(field("yyyy-MM-dd")).isEmpty)
    //default as date
    assert(DateFieldValidator.validate(field("dd.MM.yy", Option("01.05.18"))).isEmpty)
    //default as timestamp
    assert(DateFieldValidator.validate(field("yyyy/dd/MM", Option("2010/21/11 04:00:00"))).isEmpty)
  }

  test("date with time zone in pattern") {
    val expected = Set(
      ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
    )
    //no default
    assert(DateFieldValidator.validate(field("yyyy-MM-dd zz")).toSet == expected)
    //default as timestamp
    assert(DateFieldValidator.validate(field("dd.MM.yyyy+zz", Option("23.10.2000+CET"))).toSet == expected)
    //extra chars in default
    assert(DateFieldValidator.validate(field("yyMMdd_zz", Option("190301_EST!!!!"))).toSet == expected)
    //timestamp with offset time zone
    assert(DateFieldValidator.validate(field("yyyy/MM/dd XXX", Option("2019/01/31 -11:00"))).toSet == expected)
  }

  test("invalid pattern") {
    val expected1 = Set(
      ValidationError("Illegal pattern character 'f'")
    )
    assert(DateFieldValidator.validate(field("fubar")).toSet == expected1)
    val expected2 = Set(
      ValidationError("Illegal pattern character 'x'")
    )
    assert(DateFieldValidator.validate(field("yyMMdd_xx")).toSet == expected2)
  }

  test("invalid default") {
    //empty default
    val expected1 = Set(
      ValidationError("""Unparseable date: """""),
      ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
    )
    assert(DateFieldValidator.validate(field("yyMMdd_zz", Option(""))).toSet == expected1)
    //wrong default
    val expected2 = Set(
      ValidationError("""Unparseable date: "1999-12-31"""")
    )
    assert(DateFieldValidator.validate(field("yyyy/MM/dd", Option("1999-12-31"))).toSet == expected2)
    //invalid epoch default
    val expected3 = Set(
      ValidationError("'2019-01-01' cannot be cast to date")
    )
    assert(DateFieldValidator.validate(field("epoch", Option("2019-01-01"))).toSet == expected3)
    //epoch overflow
    val expected5 = Set(
      ValidationError("'8748743743948390823948239084294938231122123' cannot be cast to date")
    )
    assert(DateFieldValidator.validate(field("epoch", Option("8748743743948390823948239084294938231122123"))).toSet == expected5)
  }

  test("utilizing default time zone") {
    val pattern = "yyyy-MM-dd"
    val value = Option("2000-01-01")
    val expected = Set(
      ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
    )
    // full name
    assert(DateFieldValidator.validate(field(pattern, value, Option("Africa/Johannesburg"))).toSet == expected)
    // abbreviation
    assert(DateFieldValidator.validate(field(pattern, value, Option("CET"))).toSet == expected)
    // offset to GMT
    assert(DateFieldValidator.validate(field(pattern, value, Option("Etc/GMT-6"))).toSet == expected)
  }

  test("issues with default time zone") {
    def expected(timeZone: String): Set[ValidationIssue] = {
      val q ="\""
      Set(
        ValidationError(s"$q$timeZone$q is not a valid time zone designation"),
        ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
      )
    }
    val pattern = "yyyy-MM-dd"
    val value = Option("2000-01-01")
    // offset
    val tz1 = "-03:00"
    assert(DateFieldValidator.validate(field(pattern, value, Option(tz1))).toSet == expected(tz1))
    // empty
    val tz2 = ""
    assert(DateFieldValidator.validate(field(pattern, value, Option(tz2))).toSet == expected(tz2))
    // gibberish
    val tz3 = "Gjh878-++_?"
    assert(DateFieldValidator.validate(field(pattern, value, Option(tz3))).toSet == expected(tz3))
    // non-existing
    val tz4 = "Africa/New York"
    assert(DateFieldValidator.validate(field(pattern, value, Option(tz4))).toSet == expected(tz4))
  }

  test("warning issues: double time zone") {
    val expected = Set(
      ValidationWarning("Pattern includes time zone placeholder and default time zone is also defined (will never be used)"),
      ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
    )
    assert(DateFieldValidator.validate(field("yyyy-MM-dd XX", None, Option("CET"))).toSet == expected)
    assert(DateFieldValidator.validate(field("yyyy-MM-dd zz", None, Option("CET"))).toSet == expected)
  }

  test("warning issues: missing placeholders") {
    val expected = Set(
      ValidationWarning("No year placeholder 'yyyy' found."),
      ValidationWarning("No month placeholder 'MM' found."),
      ValidationWarning("No day placeholder 'dd' found.")
    )
    assert(DateFieldValidator.validate(field("GG")).toSet == expected)
  }

  test("warning issues: redundant placeholders") {
    val expected = Set(
      ValidationWarning("Redundant hour placeholder 'H' found."),
      ValidationWarning("Redundant minute placeholder 'm' found."),
      ValidationWarning("Redundant second placeholder 's' found."),
      ValidationWarning("Redundant millisecond placeholder 'S' found."),
      ValidationWarning("Redundant microsecond placeholder 'i' found."),
      ValidationWarning("Redundant nanosecond placeholder 'n' found."),
      ValidationWarning("Redundant am/pm placeholder 'a' found."),
      ValidationWarning("Redundant hour placeholder 'k' found."),
      ValidationWarning("Redundant hour placeholder 'h' found."),
      ValidationWarning("Redundant hour placeholder 'H' found.")
    )
    assert(DateFieldValidator.validate(field("yyyy-MM-dd HH:mm:ss.SSSiiinnn (aakkhhKK)", None, None)).toSet == expected)
  }

  test("warning issues: missing placeholders with default time zone") {
    val expected = Set(
      ValidationWarning("No year placeholder 'yyyy' found."),
      ValidationWarning("No month placeholder 'MM' found."),
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("Time zone is defined in pattern for date. While it's valid, it can lead to unexpected outcomes.")
    )
    assert(DateFieldValidator.validate(field("GG", None, Option("CET"))).toSet == expected)
  }

  test("warning issues: day placeholder wrong case") {
    val expected = Set(
      ValidationWarning("No day placeholder 'dd' found."),
      ValidationWarning("Rarely used DayOfYear placeholder 'D' found. Possibly DayOfMonth 'd' intended.")
    )
    assert(DateFieldValidator.validate(field("yyyy/MM/DD")).toSet == expected)
  }

  test("all relevant placeholders") {
    assert(DateFieldValidator.validate(field("GG yyyy MM ww W DDD dd F E")).isEmpty)
  }
}
