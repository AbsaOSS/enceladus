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

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.numeric.{DecimalSymbols, NumericPattern}
import za.co.absa.enceladus.utils.types.GlobalDefaults

import scala.util.Success

class DecimalParserSuite extends FunSuite {
  test("No pattern, no limitations") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern(decimalSymbols)
    val parser = DecimalParser(pattern)
    assert(parser.parse("3.14") == Success(BigDecimal("3.14")))
    assert(parser.parse("1.") == Success(BigDecimal.valueOf(1)))
    assert(parser.parse("-7") == Success(BigDecimal.valueOf(-7)))
    assert(parser.parse(".271E1") == Success(BigDecimal("2.71")))
    assert(parser.parse("271E-2") == Success(BigDecimal("2.71")))
  }

  test("No pattern, no limitations, minus sign and decimal separator altered") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(minusSign = 'N', decimalSeparator = ',')
    val pattern = NumericPattern(decimalSymbols)
    val parser = DecimalParser(pattern)
    assert(parser.parse("3,14") == Success(BigDecimal("3.14")))
    assert(parser.parse("1,") == Success(BigDecimal.valueOf(1)))
    assert(parser.parse("N7") == Success(BigDecimal.valueOf(-7)))
    assert(parser.parse(",271E1") == Success(BigDecimal("2.71")))
    assert(parser.parse("271EN2") == Success(BigDecimal("2.71")))
    assert(parser.parse("-11.1").isFailure)
  }

  test("Simple pattern, some limitations") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("0.#", decimalSymbols)
    val parser = DecimalParser(pattern, Some(BigDecimal.valueOf(-1000)), Some(BigDecimal.valueOf(1000)))
    assert(parser.parse("3.14") == Success(BigDecimal("3.14"))) //NB! number of hashes and 0 in pattern is not reliable
    assert(parser.parse("1.") == Success(BigDecimal.valueOf(1)))
    assert(parser.parse("-7") == Success(BigDecimal.valueOf(-7)))
    assert(parser.parse(".271E1") == Success(BigDecimal("2.71"))) //NB! number of hashes and 0 in pattern is not reliable
    assert(parser.parse("271E-2") == Success(BigDecimal("2.71")))
    assert(parser.parse("1000.0000000000000000000000000000000000000000000000000001").isFailure)
    assert(parser.parse("-1000.0000000000000000000000000000000000000000000000000001").isFailure)
  }

  test("pattern with altered decimal symbols") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(
      decimalSeparator = ',',
      groupingSeparator = ' ',
      minusSign = '~'
    )
    val pattern = NumericPattern("#,##0.000",decimalSymbols) //NB! that the standard grouping and decimal separators are used
    val parser = DecimalParser(pattern)

    assert(parser.parse("100") == Success(BigDecimal.valueOf(100)))
    assert(parser.parse("~1") == Success(BigDecimal.valueOf(-1)))
    assert(parser.parse("1 000,3") == Success(BigDecimal("1000.3")))
    assert(parser.parse("~2 000,003") == Success(BigDecimal("-2000.003")))
    assert(parser.parse("3 0000,000001") == Success(BigDecimal("30000.000001"))) // grouping size is not reliable for parsing
    assert(parser.parse("31,4E4") == Success(BigDecimal.valueOf(314000)))
    assert(parser.parse("-4").isFailure)
    assert(parser.parse("3,14E3") == Success(BigDecimal("3140")))
    assert(parser.parse("0.000 1").isFailure) // NB! grouping separator is not supported
    assert(parser.parse("3.14E3").isFailure)
    assert(parser.parse("~1 ").isFailure)
    assert(parser.parse(" ~1 ").isFailure)
  }

  test("grouping separator is not supported in decimal places") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern1 = NumericPattern("0.000,#",decimalSymbols)
    val exception = intercept[IllegalArgumentException] {
      DecimalParser(pattern1)
    }
    assert(exception.getMessage == """Malformed pattern "0.000,#"""")

    val pattern2 = NumericPattern("0.000#",decimalSymbols)
    val parser2 = DecimalParser(pattern2)
    assert(parser2.parse("0.000,1").isFailure)
  }

  test("Prefix, suffix and different negative pattern") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("Alt: 0.#Feet;Alt: (0.#)Feet",decimalSymbols)
    val parser = DecimalParser(pattern)

    assert(parser.parse("Alt: 10000.5Feet") == Success(BigDecimal("10000.5")))
    assert(parser.parse("Alt: (100)Feet") == Success(BigDecimal.valueOf(-100)))
    assert(parser.parse("Alt: 612E-2Feet") == Success(BigDecimal("6.12")))
    assert(parser.parse("Alt: 10,000Feet").isFailure)
    assert(parser.parse("100").isFailure)
  }

  test("Percent") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("#,##0.#%",decimalSymbols)
    val parser = DecimalParser(pattern)

    assert(parser.parse("113.8%") == Success(BigDecimal("1.138")))
    assert(parser.parse("-5,000.1%") == Success(BigDecimal("-50.001")))
    assert(parser.parse("113.8").isFailure)

    println(s"=${decimalSymbols.permillSign}=")
  }
}
