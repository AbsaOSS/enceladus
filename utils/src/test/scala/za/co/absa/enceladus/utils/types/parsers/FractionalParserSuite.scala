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
import za.co.absa.enceladus.utils.numeric.{DecimalSymbols, NumericPattern}
import za.co.absa.enceladus.utils.types.GlobalDefaults

import scala.util.Success

class FractionalParserSuite extends AnyFunSuite {
  private val reallyBigNumberString = "12345678901234567890123456789012345678901234567890123456789012345678901234567890" +
    "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
    "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
    "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
  private val reallySmallNumberString = s"-$reallyBigNumberString"

  test("No pattern") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern(decimalSymbols)
    val parserFloat = FractionalParser[Float](pattern, Float.MinValue, Float.MaxValue)
    val parserDouble = FractionalParser[Double](pattern, Double.MinValue, Double.MaxValue)
    assert(parserFloat.parse("3.14") == Success(3.14F))
    assert(parserDouble.parse("3.14") == Success(3.14D))
    assert(parserFloat.parse("+1.") == Success(1F))
    assert(parserDouble.parse("1.") == Success(1D))
    assert(parserFloat.parse("-7") == Success(-7F))
    assert(parserDouble.parse("-7") == Success(-7D))
    assert(parserFloat.parse(".271E1") == Success(2.71F))
    assert(parserDouble.parse(".271E1") == Success(2.71D))
    assert(parserFloat.parse("271E-2") == Success(2.71F))
    assert(parserDouble.parse("+271E-2") == Success(2.71D))
    assert(parserFloat.parse("1E40").isFailure)
    assert(parserDouble.parse("1E40") == Success(1.0E40))
    assert(parserDouble.parse("1E360").isFailure)
  }

  test("Simple pattern, some limitations") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("0.#", decimalSymbols)
    val parserFloat = FractionalParser[Float](pattern, Float.MinValue, Float.MaxValue)
    val parserDouble = FractionalParser[Double](pattern, Double.MinValue, Double.MaxValue)
    assert(parserFloat.parse("3.14") == Success(3.14F))
    assert(parserDouble.parse("3.14") == Success(3.14D))
    assert(parserFloat.parse("1.") == Success(1F))
    assert(parserDouble.parse("1.") == Success(1D))
    assert(parserFloat.parse("-7") == Success(-7F))
    assert(parserDouble.parse("-7") == Success(-7D))
    assert(parserFloat.parse(".271E1") == Success(2.71F)) //NB! number of hashes and 0 in pattern is not reliable
    assert(parserDouble.parse(".271E1") == Success(2.71D)) //NB! number of hashes and 0 in pattern is not reliable
    assert(parserFloat.parse("271E-2") == Success(2.71F))
    assert(parserDouble.parse("271E-2") == Success(2.71D))
    assert(parserFloat.parse("1E40").isFailure)
    assert(parserDouble.parse("1E40") == Success(1.0E40))
    assert(parserDouble.parse("1E360").isFailure)
  }

  test("plus doesn't work if pattern is specified") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("0", decimalSymbols)
    val parserFloat = FractionalParser[Float](pattern, Float.MinValue, Float.MaxValue)
    val parserDouble = FractionalParser[Double](pattern, Double.MinValue, Double.MaxValue)
    assert(parserFloat.parse("+2.71").isFailure)
    assert(parserDouble.parse("+2.71").isFailure)
  }

  test("infinities") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern1 = NumericPattern(decimalSymbols)
    val pattern2 = NumericPattern("0.#", decimalSymbols)
    val parserFloatStd1 = FractionalParser.withInfinity[Float](pattern1)
    val parserDoubleStd1 = FractionalParser.withInfinity[Double](pattern1)
    val parserFloatStd2 = FractionalParser.withInfinity[Float](pattern2)
    val parserDoubleStd2 = FractionalParser.withInfinity[Double](pattern2)
    assert(parserFloatStd1.parse("∞") == Success(Float.PositiveInfinity))
    assert(parserFloatStd1.parse("-∞") == Success(Float.NegativeInfinity))
    assert(parserDoubleStd1.parse("∞") == Success(Double.PositiveInfinity))
    assert(parserDoubleStd1.parse("-∞") == Success(Double.NegativeInfinity))
    assert(parserFloatStd2.parse("∞") == Success(Float.PositiveInfinity))
    assert(parserFloatStd2.parse("-∞") == Success(Float.NegativeInfinity))
    assert(parserDoubleStd2.parse("∞") == Success(Double.PositiveInfinity))
    assert(parserDoubleStd2.parse("-∞") == Success(Double.NegativeInfinity))
    assert(parserFloatStd1.parse("3E40") == Success(Float.PositiveInfinity))
    assert(parserFloatStd1.parse("-7699980973893499984399399999999999999998976876999") == Success(Float.NegativeInfinity))
    assert(parserDoubleStd1.parse(reallyBigNumberString) == Success(Double.PositiveInfinity))
    assert(parserDoubleStd1.parse(reallySmallNumberString) == Success(Double.NegativeInfinity))
    assert(parserDoubleStd1.parse("2E308") == Success(Double.PositiveInfinity))
    assert(parserDoubleStd1.parse("-6.6E666") == Success(Double.NegativeInfinity))
    assert(parserFloatStd2.parse("1276493809384398420983098239843298980977679008") == Success(Float.PositiveInfinity))
    assert(parserFloatStd2.parse("-2.71E55") == Success(Float.NegativeInfinity))
    assert(parserDoubleStd2.parse(reallyBigNumberString) == Success(Double.PositiveInfinity))
    assert(parserDoubleStd2.parse(reallySmallNumberString) == Success(Double.NegativeInfinity))
    assert(parserDoubleStd2.parse("2E308") == Success(Double.PositiveInfinity))
    assert(parserDoubleStd2.parse("-1E1000") == Success(Double.NegativeInfinity))
  }

  test("infinities redefined") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(minusSign = '&', infinityValue = "Infinity")
    val pattern1 = NumericPattern(decimalSymbols)
    val pattern2 = NumericPattern("#", decimalSymbols)
    val pattern3 = NumericPattern("#;Negative#", decimalSymbols)
    val parser1 = FractionalParser.withInfinity[Double](pattern1)
    val parser2 = FractionalParser.withInfinity[Float](pattern2)
    val parser3 = FractionalParser.withInfinity[Double](pattern3)
    assert(parser1.parse("Infinity") == Success(Double.PositiveInfinity))
    assert(parser1.parse("&Infinity") == Success(Double.NegativeInfinity))
    assert(parser2.parse("Infinity") == Success(Float.PositiveInfinity))
    assert(parser2.parse("&Infinity") == Success(Float.NegativeInfinity))
    assert(parser3.parse("Infinity") == Success(Double.PositiveInfinity))
    assert(parser3.parse("NegativeInfinity") == Success(Double.NegativeInfinity))
    assert(parser3.parse("&Infinity").isFailure)
  }

  test("No pattern, no limitations, minus sign and decimal separator altered") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(minusSign = 'N', decimalSeparator = ',')
    val pattern = NumericPattern(decimalSymbols)
    val parser = FractionalParser(pattern)
    assert(parser.parse("6,28") == Success(6.28D))
    assert(parser.parse("10000,") == Success(10000D))
    assert(parser.parse("N7") == Success(-7D))
    assert(parser.parse(",271E1") == Success(2.71D))
    assert(parser.parse("271EN2") == Success(2.71D))
    assert(parser.parse("-11.1").isFailure)
  }

  test("pattern with altered decimal symbols") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(
      decimalSeparator = ',',
      groupingSeparator = ''',
      minusSign = '@'
    )
    val pattern = NumericPattern("#,##0",decimalSymbols) //NB! that the standard grouping separator is used
    val parser = FractionalParser(pattern)

    assert(parser.parse("100") == Success(100))
    assert(parser.parse("@,1") == Success(-0.1D))
    assert(parser.parse("1'032,") == Success(1032D))
    assert(parser.parse("@2'000,55") == Success(-2000.55D))
    assert(parser.parse("3'0000,001") == Success(30000.001D)) // grouping size is not reliable for parsing
    assert(parser.parse("314E@2") == Success(3.14D))
    assert(parser.parse("-4").isFailure)
    assert(parser.parse("3.14E3").isFailure)
    assert(parser.parse("@1 ").isFailure)
    assert(parser.parse(" @1 ").isFailure)
  }

  test("Prefix, suffix and different negative pattern") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("Temperature #,##0C;Freezing -0",decimalSymbols)
    val parser = FractionalParser(pattern)

    assert(parser.parse("Temperature 100C") == Success(100D))
    assert(parser.parse("Temperature 36.8C") == Success(36.8D))
    assert(parser.parse("Freezing -12") == Success(-12D))
    assert(parser.parse("Temperature 1,234C") == Success(1234))
    assert(parser.parse("Freezing 300.0C").isFailure)
    assert(parser.parse("100.2").isFailure)
  }

  test("Percent") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("#,##0.#%",decimalSymbols)
    val parser = FractionalParser(pattern)

    assert(parser.parse("113.8%") == Success(1.138D))
    assert(parser.parse("-5,000.1%") == Success(-5000.1D / 100)) // -5000.1D / 100 = -50.001000000000005
    assert(parser.parse("113.8").isFailure)
  }

}
