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

package za.co.absa.enceladus.utils.validation

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

class DateTimeValidatorsSuite extends FunSuite {
  TimeZoneNormalizer.normalizeTimezone()

  test("epoch pattern") {
    assert(DateTimeValidators.isDateTimePatternValid("epoch").isEmpty)
    //with default
    assert(DateTimeValidators.isDateTimePatternValid("epoch", Some("5545556")).isEmpty)
  }

  test("milliepoch pattern") {
    assert(DateTimeValidators.isDateTimePatternValid("milliepoch").isEmpty)
    //with default
    assert(DateTimeValidators.isDateTimePatternValid("milliepoch", Some("55455560000")).isEmpty)
  }

  test("date pattern") {
    //no default
    assert(DateTimeValidators.isDateTimePatternValid("yyyy-MM-dd").isEmpty)
    //default as date
    assert(DateTimeValidators.isDateTimePatternValid("dd.MM.yy", Some("01.05.18"))isEmpty)
    //default as timestamp
    assert(DateTimeValidators.isDateTimePatternValid("yyyy/dd/MM", Some("2010/21/11 04:00:00")).isEmpty) //TODO Correct?
  }

  test("timestamp pattern") {
    //no default
    assert(DateTimeValidators.isDateTimePatternValid("yyyy-MM-dd hh:mm:ss").isEmpty)
    //default as timestamp
    assert(DateTimeValidators.isDateTimePatternValid("hh-mm-ss~~dd.MM.yyyy", Some("23-10-11~~31.12.2004")).isEmpty)
    //extra chars in default
    assert(DateTimeValidators.isDateTimePatternValid("hh-mm-ss~~dd.MM.yyyy", Some("23-10-11~~31.12.2004kkkkk")).isEmpty)
  }

  test("timestamp with time zone pattern") {
    //no default
    assert(DateTimeValidators.isDateTimePatternValid("yyyy-MM-dd hh:mm:ss zz").isEmpty)
    //default as timestamp
    assert(DateTimeValidators.isDateTimePatternValid("hh-mm-ss~~dd.MM.yyyy+zz", Some("23-10-11~~31.12.2004+CET")).isEmpty)
    //extra chars in default
    assert(DateTimeValidators.isDateTimePatternValid("yyMMdd_hhmmss_zz", Some("190301_194533_EST!!!!")).isEmpty)
  }

  test("invalid pattern") {
    assert(DateTimeValidators.isDateTimePatternValid("fubar").nonEmpty)
    assert(DateTimeValidators.isDateTimePatternValid("yyMMdd_hhmmss_zz_xx").nonEmpty)
  }

  test("invalid default") {
    //empty default
    assert(DateTimeValidators.isDateTimePatternValid("yyMMdd_hhmmss_zz", Some("")).nonEmpty)
    //wrong default
    assert(DateTimeValidators.isDateTimePatternValid("yyyy/MM/dd", Some("1999-12-31")).nonEmpty)
    //invalid epoch default
    assert(DateTimeValidators.isDateTimePatternValid("epoch", Some("2019-01-01")).nonEmpty)
    //timestamp pattern, date default
    assert(DateTimeValidators.isDateTimePatternValid("dd.MM.yyyy hh-mm-ss", Some("31.12.2004")).nonEmpty)
  }

}
