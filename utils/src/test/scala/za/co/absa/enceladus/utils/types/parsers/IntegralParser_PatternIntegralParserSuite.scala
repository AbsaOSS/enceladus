package za.co.absa.enceladus.utils.types.parsers

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.numeric.{DecimalSymbols, NumericPattern}
import za.co.absa.enceladus.utils.types.GlobalDefaults
import scala.util.Success

class IntegralParser_PatternIntegralParserSuite extends FunSuite {
  test("No pattern, no limitations") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern(decimalSymbols)
    val ipLong = IntegralParser[Long](pattern, None, None)
    val ipInt = IntegralParser[Int](pattern, None, None)
    val ipShort = IntegralParser[Short](pattern, None, None)
    val ipByte = IntegralParser[Byte](pattern, None, None)
    assert(ipLong.parse("98987565664") == Success(98987565664L))
    assert(ipLong.parse("-31225927393149") == Success(-31225927393149L))
    assert(ipInt.parse("2100000") == Success(2100000))
    assert(ipInt.parse("-1000") == Success(-1000))
    assert(ipShort.parse("16000") == Success(16000))
    assert(ipShort.parse("-16000") == Success(-16000))
    assert(ipByte.parse("127") == Success(127))
    assert(ipByte.parse("-17") == Success(-17))
  }

  test("No pattern, no limitations, minus sign altered") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(minusSign = 'N')
    val pattern = NumericPattern(decimalSymbols)
    val ipLong = IntegralParser[Long](pattern, None, None)
    val ipInt = IntegralParser[Int](pattern, None, None)
    val ipShort = IntegralParser[Short](pattern, None, None)
    val ipByte = IntegralParser[Byte](pattern, None, None)
    assert(ipLong.parse("98987565664") == Success(98987565664L))
    assert(ipLong.parse("N31225927393149") == Success(-31225927393149L))
    assert(ipLong.parse("-31225927393149").isFailure)
    assert(ipInt.parse("2100000") == Success(2100000))
    assert(ipInt.parse("N1000") == Success(-1000))
    assert(ipInt.parse("-1000").isFailure)
    assert(ipShort.parse("16000") == Success(16000))
    assert(ipShort.parse("N16000") == Success(-16000))
    assert(ipShort.parse("-16000").isFailure)
    assert(ipByte.parse("127") == Success(127))
    assert(ipByte.parse("N17") == Success(-17))
    assert(ipByte.parse("-17").isFailure)
  }

  test("Limit breaches") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern(decimalSymbols)
    val ipLong = IntegralParser[Long](pattern, Some(10000000000L), Some(10000000010L))
    val ipInt = IntegralParser[Int](pattern, Some(-700000), None)
    val ipShort = IntegralParser[Short](pattern, None, Some(5000))
    val ipByte = IntegralParser[Byte](pattern, None, None)
    assert(ipLong.parse("10000000011").isFailure)
    assert(ipLong.parse("9999999999").isFailure)
    assert(ipInt.parse("2147483648").isFailure)
    assert(ipInt.parse("-800000").isFailure)
    assert(ipShort.parse("5001").isFailure)
    assert(ipShort.parse("-32769").isFailure)
    assert(ipByte.parse("128").isFailure)
    assert(ipByte.parse("-129").isFailure)
  }

  test("pattern with standard decimal symbols") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("0,000",decimalSymbols)
    val parser = IntegralParser(pattern)

    assert(parser.parse("100") == Success(100))
    assert(parser.parse("-1") == Success(-1))
    assert(parser.parse("1,000") == Success(1000))
    assert(parser.parse("-2000") == Success(-2000))
    assert(parser.parse("3,0000") == Success(30000)) // grouping size is not reliable for parsing
    assert(parser.parse("314E3") == Success(314000))
    assert(parser.parse("3.14E3").isFailure)
    assert(parser.parse("-1 ").isFailure)
    assert(parser.parse(" -1 ").isFailure)
  }

  test("pattern with altered decimal symbols") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols.copy(
      decimalSeparator = ',',
      groupingSeparator = ' ',
      minusSign = '~'
    )
    val pattern = NumericPattern("#,##0",decimalSymbols) //NB! that the standard grouping separator is used
    val parser = IntegralParser(pattern)

    assert(parser.parse("100") == Success(100))
    assert(parser.parse("~1") == Success(-1))
    assert(parser.parse("1 000") == Success(1000))
    assert(parser.parse("~2 000") == Success(-2000))
    assert(parser.parse("3 0000") == Success(30000)) // grouping size is not reliable for parsing
    assert(parser.parse("314E3") == Success(314000))
    assert(parser.parse("-4").isFailure)
    assert(parser.parse("3,14E3").isFailure)
    assert(parser.parse("3.14E3").isFailure)
    assert(parser.parse("~1 ").isFailure)
    assert(parser.parse(" ~1 ").isFailure)
  }

  test("Prefix, suffix and different negative pattern") {
    val decimalSymbols: DecimalSymbols = GlobalDefaults.getDecimalSymbols
    val pattern = NumericPattern("Price: 0'EUR';Price: -0'EUR'",decimalSymbols)
    val parser = IntegralParser(pattern)

    assert(parser.parse("Price: 100EUR") == Success(100))
    assert(parser.parse("Price: -12EUR") == Success(-12))
    assert(parser.parse("Price: 1,234EUR").isFailure)
    assert(parser.parse("Price: 100.0EUR").isFailure)
    assert(parser.parse("100").isFailure)
  }
}
