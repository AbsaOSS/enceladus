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

import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class StandardizationInterpreter_IntegralSuite extends FunSuite with SparkTestBase with LoggerTestBase{

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

  private def err(value: String, cnt: Int): Seq[ErrorMessage] = {
    val item = ErrorMessage.stdCastErr("src",value)
    val array = Array.fill(cnt) (item)
    array.toList
  }

  test("Under-/overflow from CSV") {
    val src = spark.read
      .option("header", "true")
      .csv(s"${pathToTestData}integral_overflow_test.csv")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "1.0"),
        ErrorMessage.stdCastErr("shortsize", "2.0"),
        ErrorMessage.stdCastErr("integersize", "3.0"),
        ErrorMessage.stdCastErr("longsize", "4.0"))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), Option(-9223372036854775808L)),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdNullErr("bytesize"),
        ErrorMessage.stdNullErr("shortsize"))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "128"),
        ErrorMessage.stdCastErr("shortsize", "32768"),
        ErrorMessage.stdCastErr("integersize", "2147483648"),
        ErrorMessage.stdCastErr("longsize", "9223372036854775808"))),
      IntegralRow("Underflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "-129"),
        ErrorMessage.stdCastErr("shortsize", "-32769"),
        ErrorMessage.stdCastErr("integersize", "-2147483649"),
        ErrorMessage.stdCastErr("longsize", "-9223372036854775809"))),
      IntegralRow("With fractions", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "3.14"),
        ErrorMessage.stdCastErr("shortsize", "2.71"),
        ErrorMessage.stdCastErr("integersize", "1.41"),
        ErrorMessage.stdCastErr("longsize", "1.5"))),
      IntegralRow("With plus sign", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("With zeros", Option(0), Option(7), Option(-1), Option(0))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from JSON text") {
    val src = spark.read.json(s"${pathToTestData}integral_overflow_test_text.json")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "1.0"),
        ErrorMessage.stdCastErr("shortsize", "2.0"),
        ErrorMessage.stdCastErr("integersize", "3.0"),
        ErrorMessage.stdCastErr("longsize", "4.0"))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), Option(-9223372036854775808L)),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdNullErr("bytesize"),
        ErrorMessage.stdNullErr("shortsize"))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "128"),
        ErrorMessage.stdCastErr("shortsize", "32768"),
        ErrorMessage.stdCastErr("integersize", "2147483648"),
        ErrorMessage.stdCastErr("longsize", "9223372036854775808"))),
      IntegralRow("Underflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "-129"),
        ErrorMessage.stdCastErr("shortsize", "-32769"),
        ErrorMessage.stdCastErr("integersize", "-2147483649"),
        ErrorMessage.stdCastErr("longsize", "-9223372036854775809"))),
      IntegralRow("With fractions", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "3.14"),
        ErrorMessage.stdCastErr("shortsize", "2.71"),
        ErrorMessage.stdCastErr("integersize", "1.41"),
        ErrorMessage.stdCastErr("longsize", "1.5"))),
      IntegralRow("With plus sign", Option(127), Option(32767), Option(2147483647), Option(9223372036854775807L)),
      IntegralRow("With zeros", Option(0), Option(7), Option(-1), Option(0))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from JSON numeric") {
    val src = spark.read.json(s"${pathToTestData}integral_overflow_test_numbers.json")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("Decimal entry", Option(0), Option(2), Option(3), Option(4), Seq(
        ErrorMessage.stdCastErr("bytesize", "1.1"))),
      IntegralRow("Full negative", Option(-128), Option(-32768), Option(-2147483648), None, Seq(
        ErrorMessage.stdCastErr("longsize", "-9223372036854776000"))),
      IntegralRow("Full positive", Option(127), Option(32767), Option(2147483647), None, Seq(
        ErrorMessage.stdCastErr("longsize", "9223372036854776000"))),
      IntegralRow("Nulls", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdNullErr("bytesize"),
        ErrorMessage.stdNullErr("shortsize"))),
      IntegralRow("One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("Overflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "128.0"),
        ErrorMessage.stdCastErr("shortsize", "32768"),
        ErrorMessage.stdCastErr("integersize", "2147483648"),
        ErrorMessage.stdCastErr("longsize", "9223372036854776000"))),
      IntegralRow("Underflow", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "-129.0"),
        ErrorMessage.stdCastErr("shortsize", "-32769"),
        ErrorMessage.stdCastErr("integersize", "-2147483649"),
        ErrorMessage.stdCastErr("longsize", "-9223372036854776000")))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow from strongly typed input - long") {
    val src = spark.createDataFrame(Seq(
      new InputRowLongsForIntegral("1-Byte", Byte.MaxValue),
      new InputRowLongsForIntegral("2-Short", Short.MaxValue),
      new InputRowLongsForIntegral("3-Int", Int.MaxValue),
      new InputRowLongsForIntegral("4-Long", Long.MaxValue),
      new InputRowLongsForIntegral("5-Byte", Byte.MinValue),
      new InputRowLongsForIntegral("6-Short", Short.MinValue),
      new InputRowLongsForIntegral("7-Int", Int.MinValue),
      new InputRowLongsForIntegral("8-Long", Long.MinValue)
    ))
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("1-Byte", Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue)),
      IntegralRow("2-Short", Option(0), Option(Short.MaxValue), Option(Short.MaxValue), Option(Short.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Short.MaxValue.toString))),
      IntegralRow("3-Int", Option(0), Option(0), Option(Int.MaxValue), Option(Int.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Int.MaxValue.toString),
        ErrorMessage.stdCastErr("shortsize", Int.MaxValue.toString))),
      IntegralRow("4-Long", Option(0), Option(0), None, Option(Long.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Long.MaxValue.toString),
        ErrorMessage.stdCastErr("shortsize", Long.MaxValue.toString),
        ErrorMessage.stdCastErr("integersize", Long.MaxValue.toString))),
      IntegralRow("5-Byte", Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue)),
      IntegralRow("6-Short", Option(0), Option(Short.MinValue), Option(Short.MinValue), Option(Short.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Short.MinValue.toString))),
      IntegralRow("7-Int", Option(0), Option(0), Option(Int.MinValue), Option(Int.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Int.MinValue.toString),
        ErrorMessage.stdCastErr("shortsize", Int.MinValue.toString))),
      IntegralRow("8-Long", Option(0), Option(0), None, Option(Long.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Long.MinValue.toString),
        ErrorMessage.stdCastErr("shortsize", Long.MinValue.toString),
        ErrorMessage.stdCastErr("integersize", Long.MinValue.toString)))
    )
    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("Under-/overflow and precision lost from strongly typed input - double") {

    val reallyBig: Double = 24578754548798454658754546785454.0
    val tinyFractionalPart: Double = 1.000000000000001
    val seq: Seq[InputRowDoublesForIntegral] = Seq(
      new InputRowDoublesForIntegral("00-One", 1),
      new InputRowDoublesForIntegral("01-Byte", Byte.MaxValue.toDouble),
      new InputRowDoublesForIntegral("02-Short", Short.MaxValue.toDouble),
      new InputRowDoublesForIntegral("03-Int", Int.MaxValue.toDouble),
      new InputRowDoublesForIntegral("04-Long", Long.MaxValue.toDouble),
      new InputRowDoublesForIntegral("05-Byte", Byte.MinValue.toDouble),
      new InputRowDoublesForIntegral("06-Short", Short.MinValue.toDouble),
      new InputRowDoublesForIntegral("07-Int", Int.MinValue.toDouble),
      new InputRowDoublesForIntegral("08-Long", Long.MinValue.toDouble),
      new InputRowDoublesForIntegral("09-Pi", Math.PI),
      new InputRowDoublesForIntegral("10-Whole", 7.00),
      new InputRowDoublesForIntegral("11-Really small", Double.MinPositiveValue),
      new InputRowDoublesForIntegral("12-Really big", reallyBig),
      new InputRowDoublesForIntegral("13-Tiny fractional part", tinyFractionalPart),
      new InputRowDoublesForIntegral("14-NaN", Double.NaN),
      InputRowDoublesForIntegral("15-Null", None, None, None, None)
    )

    val src = spark.createDataFrame(seq)
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("00-One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("01-Byte", Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue), Option(Byte.MaxValue)),
      IntegralRow("02-Short", Option(0), Option(Short.MaxValue), Option(Short.MaxValue), Option(Short.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Short.MaxValue.toDouble.toString))),
      IntegralRow("03-Int", Option(0), Option(0), Option(Int.MaxValue), Option(Int.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Int.MaxValue.toDouble.toString),
        ErrorMessage.stdCastErr("shortsize", Int.MaxValue.toDouble.toString))),
      IntegralRow("04-Long", Option(0), Option(0), None, Option(Long.MaxValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Long.MaxValue.toDouble.toString),
        ErrorMessage.stdCastErr("shortsize", Long.MaxValue.toDouble.toString),
        ErrorMessage.stdCastErr("integersize", Long.MaxValue.toDouble.toString))),
      IntegralRow("05-Byte", Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue), Option(Byte.MinValue)),
      IntegralRow("06-Short", Option(0), Option(Short.MinValue), Option(Short.MinValue), Option(Short.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Short.MinValue.toDouble.toString))),
      IntegralRow("07-Int", Option(0), Option(0), Option(Int.MinValue), Option(Int.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Int.MinValue.toDouble.toString),
        ErrorMessage.stdCastErr("shortsize", Int.MinValue.toDouble.toString))),
      IntegralRow("08-Long", Option(0), Option(0), None, Option(Long.MinValue), Seq(
        ErrorMessage.stdCastErr("bytesize", Long.MinValue.toDouble.toString),
        ErrorMessage.stdCastErr("shortsize", Long.MinValue.toDouble.toString),
        ErrorMessage.stdCastErr("integersize", Long.MinValue.toDouble.toString))),
      IntegralRow("09-Pi", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", Math.PI.toString),
        ErrorMessage.stdCastErr("shortsize", Math.PI.toString),
        ErrorMessage.stdCastErr("integersize", Math.PI.toString),
        ErrorMessage.stdCastErr("longsize", Math.PI.toString))),
      IntegralRow("10-Whole", Option(7), Option(7), Option(7), Option(7)),
      IntegralRow("11-Really small", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", Double.MinPositiveValue.toString),
        ErrorMessage.stdCastErr("shortsize", Double.MinPositiveValue.toString),
        ErrorMessage.stdCastErr("integersize", Double.MinPositiveValue.toString),
        ErrorMessage.stdCastErr("longsize", Double.MinPositiveValue.toString))),
      IntegralRow("12-Really big", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", reallyBig.toString),
        ErrorMessage.stdCastErr("shortsize", reallyBig.toString),
        ErrorMessage.stdCastErr("integersize", reallyBig.toString),
        ErrorMessage.stdCastErr("longsize", reallyBig.toString))),
      IntegralRow("13-Tiny fractional part", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", tinyFractionalPart.toString),
        ErrorMessage.stdCastErr("shortsize", tinyFractionalPart.toString),
        ErrorMessage.stdCastErr("integersize", tinyFractionalPart.toString),
        ErrorMessage.stdCastErr("longsize", tinyFractionalPart.toString))),
      IntegralRow("14-NaN", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", "NaN"),
        ErrorMessage.stdCastErr("shortsize", "NaN"),
        ErrorMessage.stdCastErr("integersize", "NaN"),
        ErrorMessage.stdCastErr("longsize", "NaN"))),
      IntegralRow("15-Null", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdNullErr("bytesize"),
        ErrorMessage.stdNullErr("shortsize")))
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

    val seq: Seq[InputRowBigDecimalsForIntegral] = Seq(
      new InputRowBigDecimalsForIntegral("00-One", 1.0),
      new InputRowBigDecimalsForIntegral("01-Pi", pi),
      new InputRowBigDecimalsForIntegral("02-Tiny fractional part", tinyFractionalPart),
      new InputRowBigDecimalsForIntegral("03-Really big", reallyBig),
      new InputRowBigDecimalsForIntegral("04-Really small", reallySmall),
      new InputRowBigDecimalsForIntegral("05-Short", shortOverflow),
      new InputRowBigDecimalsForIntegral("06-Null", null)
    )
    val src = spark.createDataFrame(seq)
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      IntegralRow("00-One", Option(1), Option(1), Option(1), Option(1)),
      IntegralRow("01-Pi", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", formatBigDecimal(pi)),
        ErrorMessage.stdCastErr("shortsize", formatBigDecimal(pi)),
        ErrorMessage.stdCastErr("integersize", formatBigDecimal(pi)),
        ErrorMessage.stdCastErr("longsize", formatBigDecimal(pi)))),
      IntegralRow("02-Tiny fractional part", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", tinyFractionalPartStr),
        ErrorMessage.stdCastErr("shortsize", tinyFractionalPartStr),
        ErrorMessage.stdCastErr("integersize", tinyFractionalPartStr),
        ErrorMessage.stdCastErr("longsize", tinyFractionalPartStr))),
      IntegralRow("03-Really big", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", reallyBigStr),
        ErrorMessage.stdCastErr("shortsize", reallyBigStr),
        ErrorMessage.stdCastErr("integersize", reallyBigStr),
        ErrorMessage.stdCastErr("longsize", reallyBigStr))),
      IntegralRow("04-Really small", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdCastErr("bytesize", reallySmallStr),
        ErrorMessage.stdCastErr("shortsize", reallySmallStr),
        ErrorMessage.stdCastErr("integersize", reallySmallStr),
        ErrorMessage.stdCastErr("longsize", reallySmallStr))),
      IntegralRow("05-Short", Option(0), Option(0), Option(Short.MaxValue + 1), Option(Short.MaxValue + 1), Seq(
        ErrorMessage.stdCastErr("bytesize", formatBigDecimal(shortOverflow)),
        ErrorMessage.stdCastErr("shortsize", formatBigDecimal(shortOverflow)))),
      IntegralRow("06-Null", Option(0), Option(0), None, None, Seq(
        ErrorMessage.stdNullErr("bytesize"),
        ErrorMessage.stdNullErr("shortsize")))
    )

    assertResult(exp)(std.as[IntegralRow].collect().sortBy(_.description).toList)
  }

  test("No pattern, but altered symbols") {
    val input = Seq(
      ("01-Normal", "3"),
      ("02-Null", null),
      ("03-Far negative", "^100000000"),
      ("04-Wrong", "hello")
    )
    val decimalSeparator = ","
    val groupingSeparator = "."
    val minusSign = "^"
    val srcField = "src"

    val src = input.toDF("description", srcField)

    val desiredSchemaWithAlters = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("bf", ByteType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("sf", ShortType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.DefaultValue, "^1")
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("if", IntegerType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("lf", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DefaultValue, "1000")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithAlters, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Normal", "3", 3, Some(3), Some(3), 3, Seq.empty),
      ("02-Null", null, 0, None, None, 1000, Array.fill(2)(ErrorMessage.stdNullErr(srcField)).toList),
      ("03-Far negative", "^100000000", 0, Some(-1), Some(-100000000), -100000000, err("^100000000", 2)),
      ("04-Wrong", "hello", 0, Some(-1), None, 1000, err("hello", 4))
    )

    assertResult(exp)(std.as[(String, String, Byte, Option[Short], Option[Int], Long, Seq[ErrorMessage])].collect().toList)
  }

  test("Using patterns") {
    val input = Seq(
      ("01-Normal", "3 feet"),
      ("02-Null", null),
      ("03-Far negative", "^100.000.000 feet"),
      ("04-Wrong", "hello"),
      ("05-Not adhering to pattern", "123,456,789 feet")
    )
    val pattern = "#,##0 feet"
    val decimalSeparator = ","
    val groupingSeparator = "."
    val minusSign = "^"
    val srcField = "src"

    val src = input.toDF("description", srcField)

    val desiredSchemaWithPatterns = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("bf", ByteType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("sf", ShortType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.DefaultValue, "^1 feet")
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("if", IntegerType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("lf", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DefaultValue, "1.000 feet")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithPatterns, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Normal", "3 feet", 3, Some(3), Some(3), 3, Seq.empty),
      ("02-Null", null, 0, None, None, 1000, Array.fill(2)(ErrorMessage.stdNullErr(srcField)).toList),
      ("03-Far negative", "^100.000.000 feet", 0, Some(-1), Some(-100000000), -100000000, err("^100.000.000 feet", 2)),
      ("04-Wrong", "hello", 0, Some(-1), None, 1000, err("hello", 4)),
      ("05-Not adhering to pattern", "123,456,789 feet", 0, Some(-1), None, 1000, err("123,456,789 feet", 4))
    )

    assertResult(exp)(std.as[(String, String, Byte, Option[Short], Option[Int], Long, Seq[ErrorMessage])].collect().toList)
  }

  test("Changed Radix") {
    val input = Seq(
      ("00-Null", null),
      ("01-Binary", "+1101"),
      ("02-Binary negative", "§1001"),
      ("03-Septary", "35"),
      ("04-Septary negative", "§103"),
      ("05-Hex", "FF"),
      ("06-Hex negative", "§A1"),
      ("07-Hex 0x", "+0xB6"),
      ("08-Hex 0x negative", "§0x3c"),
      ("09-Radix 27", "Hello"),
      ("10-Radix 27 negative", "§Mail"),
      ("11-Wrong for all", "0XoXo")
    )
    val srcField = "src"
    val minusSign = "§"

    val src = input.toDF("description", srcField)

    val desiredSchemaWithAlters = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("bf", ByteType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.Radix, "2")
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("sf", ShortType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.DefaultValue, "§13") //NB 13 is 10 in decimal base
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.Radix, "7")
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("if", IntegerType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.Radix, "16")
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("lf", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DefaultValue, "Ada") // NB Ada is 7651 in decimal base
        .putString(MetadataKeys.Radix, "27")
        .putString(MetadataKeys.MinusSign, minusSign)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithAlters, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("00-Null"             , null   , 0 , None      , None       , 7651   , Array.fill(2)(ErrorMessage.stdNullErr(srcField)).toList),
      ("01-Binary"           , "+1101", 13, Some(393) , Some(4353) , 20413  , Seq.empty),
      ("02-Binary negative"  , "§1001", -9, Some(-344), Some(-4097), -19684 , Seq.empty),
      ("03-Septary"          , "35"   , 0 , Some(26)  , Some(53)   , 86     , err("35", 1)),
      ("04-Septary negative" , "§103" , 0 , Some(-52) , Some(-259) , -732   , err("§103", 1)),
      ("05-Hex"              , "FF"   , 0 , Some(-10) , Some(255)  , 420    , err("FF", 2)),
      ("06-Hex negative"     , "§A1"  , 0 , Some(-10) , Some(-161) , -271   , err("§A1", 2)),
      ("07-Hex 0x"           , "+0xB6", 0 , Some(-10) , Some(182)  , 7651   , err("+0xB6", 3)),
      ("08-Hex 0x negative"  , "§0x3c", 0 , Some(-10) , Some(-60)  , 7651   , err("§0x3c", 3)),
      ("09-Radix 27"         , "Hello" , 0, Some(-10) , None       , 9325959, err("Hello", 3)),
      ("10-Radix 27 negative", "§Mail", 0 , Some(-10) , None       , -440823, err("§Mail", 3)),
      ("11-Wrong for all"    , "0XoXo", 0 , Some(-10) , None       , 7651   , err("0XoXo", 4))
    )

    assertResult(exp)(std.as[(String, String, Byte, Option[Short], Option[Int], Long, Seq[ErrorMessage])].collect().toList)
  }


}
