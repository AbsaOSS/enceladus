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

package za.co.absa.enceladus.standardization.interpreter

import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StandardizationInterpreter_IntegerSuite extends FunSuite with SparkTestBase{

  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  private val pathToTestData = "src/test/resources/data/"
  private val bigDecimalFormat = {
    val pattern = "0.000000000000000000" //18 decimal places
    val nf = NumberFormat.getNumberInstance(Locale.US)
    val df = nf.asInstanceOf[DecimalFormat]
    df.applyPattern(pattern)
    df
  }

  private val desiredSchema = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("bytesize", ByteType, nullable = false),
    StructField("shortsize", ShortType, nullable = false),
    StructField("integersize", IntegerType, nullable = true),
    StructField("longsize", LongType, nullable = true)
  ))

  test("Under-/overflow from CSV") {
    val src = spark.read
      .option("header", "true")
      .csv(s"${pathToTestData}integral_overflow_test.csv")
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("1.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("2.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("3.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("4.0")))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), Option(-9223372036854775808L)),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "bytesize", Seq("null")),
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "shortsize", Seq("null")))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("128")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("32768")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("2147483648")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("9223372036854775808")))),
      IntegralRow("Underflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("-129")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("-32769")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("-2147483649")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("-9223372036854775809")))),
      IntegralRow("With fractions", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("3.14")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("2.71")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("1.41")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("1.5")))),
      IntegralRow("With plus sign", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("With zeros", Option(0), Option(7), Option(-1), Option(0))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from JSON text") {
    val src = spark.read.json(s"${pathToTestData}integral_overflow_test_text.json")
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("1.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("2.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("3.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("4.0")))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), Option(-9223372036854775808L)),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "bytesize", Seq("null")),
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "shortsize", Seq("null")))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("128")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("32768")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("2147483648")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("9223372036854775808")))),
      IntegralRow("Underflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("-129")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("-32769")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("-2147483649")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("-9223372036854775809")))),
      IntegralRow("With fractions", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("3.14")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("2.71")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("1.41")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("1.5")))),
      IntegralRow("With plus sign", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("With zeros", Option(0), Option(7), Option(-1), Option(0))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from JSON numeric") {
    val src = spark.read.json(s"${pathToTestData}integral_overflow_test_numbers.json")
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(2), Option(3), Option(4), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("1.1")))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("-9223372036854776000")))),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("9223372036854776000")))),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "bytesize", Seq("null")),
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "shortsize", Seq("null")))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("128.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("32768")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("2147483648")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("9223372036854776000")))),
      IntegralRow("Underflow", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("-129.0")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("-32769")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("-2147483649")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("-9223372036854776000"))))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from strongly typed input - long") {
    val src = spark.createDataFrame(Seq(
      new InputRowLongs("1-Byte", Byte.MaxValue),
      new InputRowLongs("2-Short", Short.MaxValue),
      new InputRowLongs("3-Int", Int.MaxValue),
      new InputRowLongs("4-Long", Long.MaxValue),
      new InputRowLongs("5-Byte", Byte.MinValue),
      new InputRowLongs("6-Short", Short.MinValue),
      new InputRowLongs("7-Int", Int.MinValue),
      new InputRowLongs("8-Long", Long.MinValue)
    ))
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("1-Byte", Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue)),
      IntegralRow("2-Short", Option(0), Option(Short.MaxValue), Option(Short.MaxValue), Option(Short.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Short.MaxValue.toString)))),
      IntegralRow("3-Int", Option(0), Option(0), Option(Int.MaxValue), Option(Int.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Int.MaxValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Int.MaxValue.toString)))),
      IntegralRow("4-Long", Option(0), Option(0), Option(0), Option(Long.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Long.MaxValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Long.MaxValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Long.MaxValue.toString)))),
      IntegralRow("5-Byte", Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue)),
      IntegralRow("6-Short", Option(0), Option(Short.MinValue), Option(Short.MinValue), Option(Short.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Short.MinValue.toString)))),
      IntegralRow("7-Int", Option(0), Option(0), Option(Int.MinValue), Option(Int.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Int.MinValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Int.MinValue.toString)))),
      IntegralRow("8-Long", Option(0), Option(0), Option(0), Option(Long.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Long.MinValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Long.MinValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Long.MinValue.toString))))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow and precision lost from strongly typed input - double") {

    val reallyBig: Double = 24578754548798454658754546785454.0
    val tinyFractionalPart: Double = 1.000000000000001
    val seq: Seq[InputRowDoubles] = Seq(
      new InputRowDoubles("00-One", 1),
      new InputRowDoubles("01-Byte", Byte.MaxValue.toDouble),
      new InputRowDoubles("02-Short", Short.MaxValue.toDouble),
      new InputRowDoubles("03-Int", Int.MaxValue.toDouble),
      new InputRowDoubles("04-Long", Long.MaxValue.toDouble),
      new InputRowDoubles("05-Byte", Byte.MinValue.toDouble),
      new InputRowDoubles("06-Short", Short.MinValue.toDouble),
      new InputRowDoubles("07-Int", Int.MinValue.toDouble),
      new InputRowDoubles("08-Long", Long.MinValue.toDouble),
      new InputRowDoubles("09-Pi", Math.PI),
      new InputRowDoubles("10-Whole", 7.00),
      new InputRowDoubles("11-Really small", Double.MinPositiveValue),
      new InputRowDoubles("12-Really big", reallyBig),
      new InputRowDoubles("13-Tiny fractional part", tinyFractionalPart),
      new InputRowDoubles("14-NaN", Double.NaN),
      InputRowDoubles("15-Null", None, None, None, None)
    )

    val src = spark.createDataFrame(seq)
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("00-One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("01-Byte", Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue)),
      IntegralRow("02-Short", Option(0), Option(Short.MaxValue), Option(Short.MaxValue), Option(Short.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Short.MaxValue.toDouble.toString)))),
      IntegralRow("03-Int", Option(0), Option(0), Option(Int.MaxValue), Option(Int.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Int.MaxValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Int.MaxValue.toDouble.toString)))),
      IntegralRow("04-Long", Option(0), Option(0), Option(0), Option(Long.MaxValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Long.MaxValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Long.MaxValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Long.MaxValue.toDouble.toString)))),
      IntegralRow("05-Byte", Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue)),
      IntegralRow("06-Short", Option(0), Option(Short.MinValue), Option(Short.MinValue), Option(Short.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Short.MinValue.toDouble.toString)))),
      IntegralRow("07-Int", Option(0), Option(0), Option(Int.MinValue), Option(Int.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Int.MinValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Int.MinValue.toDouble.toString)))),
      IntegralRow("08-Long", Option(0), Option(0), Option(0), Option(Long.MinValue), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Long.MinValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Long.MinValue.toDouble.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Long.MinValue.toDouble.toString)))),
      IntegralRow("09-Pi", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Math.PI.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Math.PI.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Math.PI.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(Math.PI.toString)))),
      IntegralRow("10-Whole", Option(7), Option(7), Option(7), Option(7)),
      IntegralRow("11-Really small", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(Double.MinPositiveValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(Double.MinPositiveValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(Double.MinPositiveValue.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(Double.MinPositiveValue.toString)))),
      IntegralRow("12-Really big", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(reallyBig.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(reallyBig.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(reallyBig.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(reallyBig.toString)))),
      IntegralRow("13-Tiny fractional part", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(tinyFractionalPart.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(tinyFractionalPart.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(tinyFractionalPart.toString)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(tinyFractionalPart.toString)))),
      IntegralRow("14-NaN", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq("NaN")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq("NaN")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq("NaN")),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq("NaN")))),
      IntegralRow("15-Null", Option(0), Option(0), None, None, Seq(
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "bytesize", Seq("null")),
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "shortsize", Seq("null"))))
    )

    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow and precision lost  from strongly typed input - decimal") {
    def formatBigDecimal(bd: BigDecimal): String = {
      bigDecimalFormat.format(bd)
    }

    val pi: BigDecimal = Math.PI
    val tinyFractionalPart: BigDecimal = BigDecimal("1.000000000000000001")
    val reallyBig: BigDecimal = BigDecimal(Long.MaxValue)*2
    val reallySmall: BigDecimal = BigDecimal(Long.MinValue)*2
    val shortOverflow: BigDecimal = Short.MaxValue + 1

    //formating is nto prices for these
    val tinyFractionalPartStr = "1.000000000000000001"
    val reallyBigStr = "18446744073709551614.000000000000000000"
    val reallySmallStr = "-18446744073709551616.000000000000000000"

    val seq: Seq[InputRowBigDecimals] = Seq(
      new InputRowBigDecimals("00-One", 1.0),
      new InputRowBigDecimals("01-Pi", pi),
      new InputRowBigDecimals("02-Tiny fractional part", tinyFractionalPart),
      new InputRowBigDecimals("03-Really big", reallyBig),
      new InputRowBigDecimals("04-Really small", reallySmall),
      new InputRowBigDecimals("05-Short", shortOverflow),
      new InputRowBigDecimals("06-Null", null)
    )
    val src = spark.createDataFrame(seq)
    showDateFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDateFrame(std)

    val exp = Seq(
      IntegralRow("00-One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("01-Pi", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(formatBigDecimal(pi))),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(formatBigDecimal(pi))),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(formatBigDecimal(pi))),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(formatBigDecimal(pi))))),
      IntegralRow("02-Tiny fractional part", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(tinyFractionalPartStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(tinyFractionalPartStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(tinyFractionalPartStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(tinyFractionalPartStr)))),
      IntegralRow("03-Really big", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(reallyBigStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(reallyBigStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(reallyBigStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(reallyBigStr)))),
      IntegralRow("04-Really small", Option(0), Option(0), Option(0), Option(0), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(reallySmallStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(reallySmallStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "integersize", Seq(reallySmallStr)),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "longsize", Seq(reallySmallStr)))),
      IntegralRow("05-Short", Option(0), Option(0), Option(Short.MaxValue + 1), Option(Short.MaxValue + 1), Seq(
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "bytesize", Seq(formatBigDecimal(shortOverflow))),
        ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "shortsize", Seq(formatBigDecimal(shortOverflow))))),
      IntegralRow("06-Null", Option(0), Option(0), None, None, Seq(
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "bytesize", Seq("null")),
        ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "shortsize", Seq("null"))))
    )

    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

}

case class IntegralRow(
                        description: String,
                        byteSize: Option[Byte],
                        shortSize: Option[Short],
                        integerSize: Option[Int],
                        longSize: Option[Long],
                        errCol: Seq[ErrorMessage] = Seq.empty
                      )

case class InputRowLongs(
                          description: String,
                          bytesize: Long,
                          shortsize: Long,
                          integersize: Long,
                          longsize: Long
                        ) {
  def this(description: String, value: Long) = {
    this(description, value, value, value, value)
  }
}

case class InputRowDoubles(
                            description: String,
                            bytesize: Option[Double],
                            shortsize: Option[Double],
                            integersize: Option[Double],
                            longsize: Option[Double]
                          ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value), Option(value), Option(value))
  }
}

case class InputRowBigDecimals(
                                description: String,
                                bytesize: Option[BigDecimal],
                                shortsize: Option[BigDecimal],
                                integersize: Option[BigDecimal],
                                longsize: Option[BigDecimal]
                              ) {
  def this(description: String, value: BigDecimal) = {
    this(description, Option(value), Option(value), Option(value), Option(value))
  }
}
