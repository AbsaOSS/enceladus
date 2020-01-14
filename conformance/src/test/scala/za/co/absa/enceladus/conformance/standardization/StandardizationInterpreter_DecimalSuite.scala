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

package za.co.absa.enceladus.conformance.standardization

import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class StandardizationInterpreter_DecimalSuite extends FunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  private val desiredSchema = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("small", DecimalType(5,2), nullable = false),
    StructField("big", DecimalType(38,18), nullable = true)
  ))

  private val zero = BigDecimal("0E-18")
  private val bigDecimalFormat = {
    val pattern = "0.000000000000000000" //18 decimal places
    val nf = NumberFormat.getNumberInstance(Locale.US)
    val df = nf.asInstanceOf[DecimalFormat]
    df.applyPattern(pattern)
    df
  }

  private def bd(number: Double): BigDecimal = {
    val s: String =  bigDecimalFormat.format(number)
    BigDecimal(s)
  }

  test("From String") {
    val seq = Seq(
      ("01-Pi", "3.14", "3.14"),
      ("02-Null", null, null),
      ("03-Long", Long.MaxValue.toString, Long.MinValue.toString),
      ("04-infinity", "-Infinity", "Infinity"),
      ("05-Really big", "123456789123456791245678912324789123456789123456789.12",
        "12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345"
        + "678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456"
        + "789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123"
        + "456789123456789123456789123456789123456789123456789123456789.1"),
      ("06-Text", "foo", "bar"),
      ("07-Exponential notation", "-1.23E2", "+9.8765E-4"),
      ("08-Small overflow", "1000", "1000"),
      ("09-Loss of precision", "123.456", "123.456")
    )
    val src = seq.toDF("description","small", "big")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      DecimalRow("01-Pi", Option(bd(3.14)), Option(bd(3.14))),
      DecimalRow("02-Null", Option(zero), None, Seq(
        ErrorMessage.stdNullErr("small"))),
      DecimalRow("03-Long", Option(zero), Option(Long.MinValue), Seq(
        ErrorMessage.stdCastErr("small", Long.MaxValue.toString))),
      DecimalRow("04-infinity", Option(zero), None,  Seq(
        ErrorMessage.stdCastErr("small", "-Infinity"),
        ErrorMessage.stdCastErr("big", "Infinity"))),
      DecimalRow("05-Really big", Option(zero), None, Seq(
        ErrorMessage.stdCastErr("small", "123456789123456791245678912324789123456789123456789.12"),
        ErrorMessage.stdCastErr("big", "1234567891234567912456789123247891234567891234567891234567891"
          + "2345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678"
          + "9123456789123456789123456789123456789123467891234567891234567891234567891234567912456789123247891234567891"
          + "2345678912345678912345678912345679124567891232478912345678912345678912345678912345678912345678912345678912"
          + "3456789.1"))),
      DecimalRow("06-Text", Option(zero), None, Seq(
        ErrorMessage.stdCastErr("small", "foo"),
        ErrorMessage.stdCastErr("big", "bar"))),
      DecimalRow("07-Exponential notation", Option(bd(-123)), Option(bd(0.00098765))),
      DecimalRow("08-Small overflow", Option(zero), Option(bd(1000)), Seq(
        ErrorMessage.stdCastErr("small", "1000"))),
      DecimalRow("09-Loss of precision", Option(bd(123.46)), Option(bd(123.456)))
    )

    assertResult(exp)(std.as[DecimalRow].collect().sortBy(_.description).toList)
  }

  test("From double") {
    val reallyBig = Double.MaxValue
    val seq = Seq(
      new InputRowDoublesForDecimal("01-Pi", Math.PI),
      InputRowDoublesForDecimal("02-Null", None, None),
      InputRowDoublesForDecimal("03-Long", Option(Long.MaxValue.toFloat), Option(Long.MinValue.toDouble)),
      InputRowDoublesForDecimal("04-Infinity", Option(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      new InputRowDoublesForDecimal("05-Really big", reallyBig),
      InputRowDoublesForDecimal("06-NaN", Option(Float.NaN), Option(Double.NaN))
    )
    val src = spark.createDataFrame(seq)
    logDataFrameContent(src)

    val exp = Seq(
      DecimalRow("01-Pi", Option(bd(3.14)), Option(bd(Math.PI))), //NB! Note the loss of precision in Pi
      DecimalRow("02-Null", Option(zero), None, Seq(
        ErrorMessage.stdNullErr("small"))),
      DecimalRow("03-Long", Option(zero), Option(-9223372036854776000.0), Seq(  // rounding in doubles for large integers
        ErrorMessage.stdCastErr("small", "9.223372036854776E18"))),
      DecimalRow("04-Infinity", Option(zero), None,  Seq(
        ErrorMessage.stdCastErr("small", "-Infinity"),
        ErrorMessage.stdCastErr("big", "Infinity"))),
      DecimalRow("05-Really big", Option(zero), None, Seq(
        ErrorMessage.stdCastErr("small", reallyBig.toString),
      ErrorMessage.stdCastErr("big", reallyBig.toString))),
      DecimalRow("06-NaN", Option(zero), None, Seq(
        ErrorMessage.stdCastErr("small", "NaN"),
        ErrorMessage.stdCastErr("big", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DecimalRow].collect().sortBy(_.description).toList)
  }

  test("No pattern, but altered symbols") {
    val input = Seq(
      ("01-Normal", "123:456"),
      ("02-Null", null),
      ("03-Far negative", "N100000000:999"),
      ("04-Wrong", "hello"),
      ("05-Not adhering to pattern", "123456.789")
    )

    val decimalSeparator = ":"
    val minusSign = "N"
    val srcField = "src"

    val src = input.toDF("description", srcField)

    val desiredSchemaWithAlters = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("small", DecimalType(5,2), nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .putString(MetadataKeys.SourceColumn, srcField)
        .build()),
      StructField("big", DecimalType(38,18), nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .putString(MetadataKeys.DefaultValue, "N1:1")
        .putString(MetadataKeys.SourceColumn, srcField)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithAlters, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Normal", "123:456", BigDecimal("123.460000000000000000"), BigDecimal("123.456000000000000000"), Seq.empty), //NB the rounding in the small
      ("02-Null", null,  BigDecimal("0E-18"), null, Seq(ErrorMessage.stdNullErr(srcField))),
      ("03-Far negative", "N100000000:999",  BigDecimal("0E-18"), BigDecimal("-100000000.999000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"N100000000:999"))),
      ("04-Wrong", "hello",  BigDecimal("0E-18"), BigDecimal("-1.100000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"hello"), ErrorMessage.stdCastErr(srcField,"hello"))),
      ("05-Not adhering to pattern", "123456.789",  BigDecimal("0E-18"), BigDecimal("-1.100000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"123456.789"), ErrorMessage.stdCastErr(srcField,"123456.789")))
    )

    assertResult(exp)(std.as[(String, String, BigDecimal, BigDecimal, Seq[ErrorMessage])].collect().toList)
  }

  test("Using patterns") {
    val input = Seq(
      ("01-Normal", "123.4‰"),
      ("02-Null", null),
      ("03-Big", "100,000,000.999‰"),
      ("04-Wrong", "hello"),
      ("05-Not adhering to pattern", "123456.789")
    )

    val pattern = "#,##0.##‰"
    val srcField = "src"

    val src = input.toDF("description", srcField)

    val desiredSchemaWithPatterns = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("small", DecimalType(5,2), nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .build()),
      StructField("big", DecimalType(38,18), nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.DefaultValue, "1,000‰")
        .putString(MetadataKeys.SourceColumn, srcField)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithPatterns, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Normal", "123.4‰", BigDecimal("0.120000000000000000"), BigDecimal("0.123400000000000000"), Seq.empty),
      ("02-Null", null,  BigDecimal("0E-18"), null, Seq(ErrorMessage.stdNullErr(srcField))),
      ("03-Big", "100,000,000.999‰",  BigDecimal("0E-18"), BigDecimal("100000.000999000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"100,000,000.999‰"))),
      ("04-Wrong", "hello",  BigDecimal("0E-18"), BigDecimal("1.000000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"hello"), ErrorMessage.stdCastErr(srcField,"hello"))),
      ("05-Not adhering to pattern", "123456.789",  BigDecimal("0E-18"), BigDecimal("1.000000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"123456.789"), ErrorMessage.stdCastErr(srcField,"123456.789")))
    )

    assertResult(exp)(std.as[(String, String, BigDecimal, BigDecimal, Seq[ErrorMessage])].collect().toList)
  }

  test("Pattern with symbols alterated") {
    val input = Seq(
      ("01-Normal", "9 123,4"),
      ("02-Null", null),
      ("03-Big", "100 000 000,999"),
      ("04-Wrong", "hello"),
      ("05-Not adhering to pattern", "123456.789"),
      ("06-Negative", "~54 123,789")
    )

    val srcField = "src"
    val decimalSeparator = ","
    val groupingSeparator = " "
    val minusSign = "~"
    val pattern = "#,##0.#" // NB the default symbols, not the redefined ones

    val src = input.toDF("description", srcField)

    val desiredSchemaWithPatterns = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("small", DecimalType(7,2), nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .putString(MetadataKeys.SourceColumn, srcField)
        .build()),
      StructField("big", DecimalType(38,18), nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .putString(MetadataKeys.DefaultValue, "1,000")
        .putString(MetadataKeys.SourceColumn, srcField)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithPatterns, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Normal", "9 123,4", BigDecimal("9123.400000000000000000"), BigDecimal("9123.400000000000000000"), Seq.empty),
      ("02-Null", null,  BigDecimal("0E-18"), null, Seq(ErrorMessage.stdNullErr(srcField))),
      ("03-Big", "100 000 000,999",  BigDecimal("0E-18"), BigDecimal("100000000.999000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"100 000 000,999"))),
      ("04-Wrong", "hello",  BigDecimal("0E-18"), BigDecimal("1.000000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"hello"), ErrorMessage.stdCastErr(srcField,"hello"))),
      ("05-Not adhering to pattern", "123456.789",  BigDecimal("0E-18"), BigDecimal("1.000000000000000000"), Seq(ErrorMessage.stdCastErr(srcField,"123456.789"), ErrorMessage.stdCastErr(srcField,"123456.789"))),
      ("06-Negative", "~54 123,789", BigDecimal("-54123.790000000000000000"), BigDecimal("-54123.789000000000000000"), Seq.empty)
    )

    assertResult(exp)(std.as[(String, String, BigDecimal, BigDecimal, Seq[ErrorMessage])].collect().toList)
  }

}
