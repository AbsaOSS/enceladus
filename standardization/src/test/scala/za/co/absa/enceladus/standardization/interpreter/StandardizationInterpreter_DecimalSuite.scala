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
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StandardizationInterpreter_DecimalSuite extends FunSuite with SparkTestBase {
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
        "12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123456789123456789.1"),
      ("06-Text", "foo", "bar"),
      ("07-Exponential notation", "-1.23E2", "+9.8765E-4"),
      ("08-Small overflow", "1000", "1000"),
      ("09-Loss of precision", "123.456", "123.456")
    )
    val src = seq.toDF("description","small", "big")
    showDataFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDataFrame(std)

    val exp = Seq(
      DecimalRow("01-Pi", Option(bd(3.14)), Option(bd(3.14))),
      DecimalRow("02-Null", Option(zero), None, Seq(
        ErrorMessage.stdNullErr("small"))),
      DecimalRow("03-Long", Option(zero), Option(Long.MinValue), Seq(
        ErrorMessage.stdCastErr("small", Long.MaxValue.toString))),
      DecimalRow("04-infinity", Option(zero), Option(zero),  Seq(
        ErrorMessage.stdCastErr("small", "-Infinity"),
        ErrorMessage.stdCastErr("big", "Infinity"))),
      DecimalRow("05-Really big", Option(zero), Option(zero), Seq(
        ErrorMessage.stdCastErr("small", "123456789123456791245678912324789123456789123456789.12"),
        ErrorMessage.stdCastErr("big", "12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123456789123456789.1"))),
      DecimalRow("06-Text", Option(zero), Option(zero), Seq(
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
    showDataFrame(src)

    val exp = Seq(
      DecimalRow("01-Pi", Option(bd(3.14)), Option(bd(Math.PI))),
      DecimalRow("02-Null", Option(zero), None, Seq(
        ErrorMessage.stdNullErr("small"))),
      DecimalRow("03-Long", Option(zero), Option(-9223372036854776000.0), Seq(  // rounding in doubles for large integers
        ErrorMessage.stdCastErr("small", "9.223372036854776E18"))),
      DecimalRow("04-Infinity", Option(zero), Option(zero),  Seq(
        ErrorMessage.stdCastErr("small", "-Infinity"),
        ErrorMessage.stdCastErr("big", "Infinity"))),
      DecimalRow("05-Really big", Option(zero), Option(zero), Seq(
        ErrorMessage.stdCastErr("small", reallyBig.toString),
      ErrorMessage.stdCastErr("big", reallyBig.toString))),
      DecimalRow("06-NaN", Option(zero), Option(zero), Seq(
        ErrorMessage.stdCastErr("small", "NaN"),
        ErrorMessage.stdCastErr("big", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDataFrame(std)

    assertResult(exp)(std.as[DecimalRow].collect().sortBy(_.description).toList)  }
}

case class DecimalRow(
                          description: String,
                          small: Option[BigDecimal],
                          big: Option[BigDecimal],
                          errCol: Seq[ErrorMessage] = Seq.empty
                        )

case class InputRowDoublesForDecimal(
                                      description: String,
                                      small: Option[Double],
                                      big: Option[Double]
                                    ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}
