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

package za.co.absa.enceladus.utils.types.parsers

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.numeric.Radix
import za.co.absa.enceladus.utils.numeric.Radix.RadixFormatException
import za.co.absa.enceladus.utils.types.parsers.NumericParser.NumericParserException

import scala.util.Success

class IntegralParser_RadixIntegralParserSuite extends AnyFunSuite {

  test("base 10 parsing succeeds") {
    val parser = IntegralParser.ofRadix(Radix(10))
    assert(parser.parse("1111") == Success(1111))
    assert(parser.parse("-1") == Success(-1))
  }

  test("base 10 parsing fails on too big number") {
    val parser = IntegralParser.ofRadix(Radix(10))
    val tooBig = "45455782147845454874654658875324"
    val fail = parser.parse(tooBig).failed.get
    assert(fail.isInstanceOf[NumberFormatException])
    assert(fail.getMessage == """For input string: """" + tooBig + """"""")
  }

  test("base 10 parsing fails on wrong input") {
    val parser = IntegralParser.ofRadix(Radix(10))
    val wrong = "Hello"
    val fail = parser.parse(wrong).failed.get
    assert(fail.isInstanceOf[NumberFormatException])
    assert(fail.getMessage == """For input string: """" + wrong + """"""")
  }

  test("base 16 parsing succeeds") {
    val parser = IntegralParser.ofRadix(Radix(16))
    assert(parser.parse("CAFE") == Success(51966))
    assert(parser.parse("-a") == Success(-10))
    assert(parser.parse("0xFFFfF") == Success(1048575))
    assert(parser.parse("+0X1A") == Success(26))
    assert(parser.parse("-0X1") == Success(-1))
    assert(parser.parse("7FFFFFFFFFFFFFFF") == Success(9223372036854775807L))
  }

  test("base 16 parsing fails on incomplete input") {
    val parser = IntegralParser.ofRadix(Radix(16))
    val fail1 = parser.parse("").failed.get
    assert(fail1.isInstanceOf[NumberFormatException])
    assert(fail1.getMessage == "Zero length BigInteger")
    val fail2 = parser.parse("0x").failed.get
    assert(fail2.isInstanceOf[NumberFormatException])
    assert(fail2.getMessage == "Zero length BigInteger")
  }

  test("base 16 parsing fails on too big input") {
    val parser = IntegralParser.ofRadix(Radix(16))
    val tooBig = "8FFFFFFFFFFFFFFF"
    val fail = parser.parse(tooBig).failed.get
    assert(fail.isInstanceOf[NumericParserException])
    assert(fail.getMessage == s"The number '$tooBig' is out of range <Some(-9223372036854775808), Some(9223372036854775807)>")
  }

  test("base 16 parsing fails on bad input") {
    val parser = IntegralParser.ofRadix(Radix(16))
    val wrong = "g"
    val fail = parser.parse(wrong).failed.get
    assert(fail.isInstanceOf[NumberFormatException])
    assert(fail.getMessage == """For input string: "g"""")
  }

  test("base 2 parsing succeeds") {
    val parser = IntegralParser.ofRadix(Radix(2))
    assert(parser.parse("1" * 63) == Success(Long.MaxValue))
    assert(parser.parse("-10101") == Success(-21))
  }

  test("base 2 parsing fails on too big number") {
    val parser = IntegralParser.ofRadix(Radix(2))
    val tooBig = "1" * 64
    val result = parser.parse(tooBig)
    val fail = result.failed.get
    assert(fail.isInstanceOf[NumericParserException])
    assert(fail.getMessage == s"The number '$tooBig' is out of range <Some(-9223372036854775808), Some(9223372036854775807)>")
  }

  test("base 2 parsing fails on wrong input") {
    val parser = IntegralParser.ofRadix(Radix(2))
    val wrong = "3"
    val fail = parser.parse(wrong).failed.get
    assert(fail.isInstanceOf[NumberFormatException])
    assert(fail.getMessage == """For input string: """" + wrong + """"""")
  }


  test("base 36 parsing succeeds") {
    val parser = IntegralParser.ofRadix(Radix(36))
    assert(parser.parse("Zardoz1") == Success(76838032045L))
    assert(parser.parse("-Wick3") == Success(-54603795))
  }

  test("base 36 parsing fails on too big number") {
    val parser = IntegralParser.ofRadix(Radix(36))
    val tooBig = "DowningStreet10"
    val result = parser.parse(tooBig)
    val fail = result.failed.get
    assert(fail.isInstanceOf[NumericParserException])
    assert(fail.getMessage == s"The number '$tooBig' is out of range <Some(-9223372036854775808), Some(9223372036854775807)>")
  }

  test("base 36 parsing fails on wrong input") {
    val parser = IntegralParser.ofRadix(Radix(36))
    val wrong = "__"
    val fail = parser.parse(wrong).failed.get
    assert(fail.isInstanceOf[NumberFormatException])
    assert(fail.getMessage == """For input string: """" + wrong + """"""")
  }

  test("string base inputs") {
    assert(IntegralParser.ofStringRadix("").radix.value == 10)
    assert(IntegralParser.ofStringRadix("DEC").radix.value == 10)
    assert(IntegralParser.ofStringRadix("decImal").radix.value == 10)
    assert(IntegralParser.ofStringRadix("Hex").radix.value == 16)
    assert(IntegralParser.ofStringRadix("HexaDecimal").radix.value == 16)
    assert(IntegralParser.ofStringRadix("bin").radix.value == 2)
    assert(IntegralParser.ofStringRadix("binarY").radix.value == 2)
    assert(IntegralParser.ofStringRadix("oct").radix.value == 8)
    assert(IntegralParser.ofStringRadix("OCTAL").radix.value == 8)
    assert(IntegralParser.ofStringRadix("23").radix.value == 23)
  }

  test("base out of range") {
    val exception1 = intercept[RadixFormatException] {
      IntegralParser.ofRadix(Radix(0))
    }
    assert(exception1.getMessage == "Radix has to be greater then 0, 0 was entered")
    val exception2 = intercept[RadixFormatException] {
      IntegralParser.ofRadix(Radix(37))
    }
    assert(exception2.getMessage == "Maximum supported radix is 36, 37 was entered")
  }

  test("base not recognized") {
    val exception = intercept[RadixFormatException] {
      IntegralParser.ofStringRadix("hello")
    }
    assert(exception.getMessage == "'hello' was not recognized as a Radix value")
  }

  test("base is smaller then 10 and minus is a higher digit") {
    val decimalSymbols =  NumericParser.defaultDecimalSymbols.copy(minusSign = '5')

    val parser = IntegralParser.ofRadix(Radix(5), decimalSymbols)
    assert(parser.parse("4321") == Success(586))
    assert(parser.parse("54321") == Success(-586))
    assert(parser.parse("-4321").isFailure)
  }

}
