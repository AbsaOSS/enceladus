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

import org.apache.spark.sql.types.{DoubleType, FloatType, MetadataBuilder, StringType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StandardizationInterpreter_FractionalSuite extends FunSuite with SparkTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  private val desiredSchema = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("floatField", FloatType, nullable = false),
    StructField("doubleField", DoubleType, nullable = true)
  ))

  private val desiredSchemaWithInfinity = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder().putBoolean("allowinfinity", value =true).build),
    StructField("doubleField", DoubleType, nullable = true,
      new MetadataBuilder().putBoolean("allowinfinity", value = true).build)
  ))

  test("From String") {
    val seq = Seq(
      ("01-Pi", "3.14", "3.14"),
      ("02-Null", null, null),
      ("03-Long", Long.MaxValue.toString, Long.MinValue.toString),
      ("04-infinity", "-Infinity", "Infinity"),
      ("05-Really big", "123456789123456791245678912324789123456789123456789.12",
        "12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123456789123456789.1"),
      ("06-Text", "foo", "bar"),
      ("07-Exponential notation", "-1.23E4", "+9.8765E-3")
    )
    val src = seq.toDF("description","floatField", "doubleField")
    showDataFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDataFrame(std)

    val exp = Seq(
      FractionalRow("01-Pi", Option(3.14F), Option(3.14)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-infinity", Option(0), Option(0),  Seq(
        ErrorMessage.stdCastErr("floatField", "-Infinity"),
        ErrorMessage.stdCastErr("doubleField", "Infinity"))),
      FractionalRow("05-Really big", Option(0), Option(0), Seq(
        ErrorMessage.stdCastErr("floatField", "123456789123456791245678912324789123456789123456789.12"),
        ErrorMessage.stdCastErr("doubleField", "12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123456789123456789.1"))),
      FractionalRow("06-Text", Option(0), Option(0), Seq(
        ErrorMessage.stdCastErr("floatField", "foo"),
        ErrorMessage.stdCastErr("doubleField", "bar"))),
      FractionalRow("07-Exponential notation", Option(-12300.0f), Option(0.0098765))
    )

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("From Long") {
    val value = 1984
    val seq = Seq(
      InputRowLongsForFractional("01-Null", None, None),
      InputRowLongsForFractional("02-Big Long", Option(Long.MaxValue.toFloat), Option(Long.MinValue.toDouble)),
      InputRowLongsForFractional("03-Long", Option(-value), Option(value))
    )
    val src = spark.createDataFrame(seq)
    showDataFrame(src)

    val exp = Seq(
      FractionalRow("01-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("02-Big Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("03-Long", Option(-value.toFloat), Option(value.toDouble))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDataFrame(std)

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("From Double") {
    val reallyBig = Double.MaxValue
    val seq = Seq(
      new InputRowDoublesForFractional("01-Pi", Math.PI),
      InputRowDoublesForFractional("02-Null", None, None),
      InputRowDoublesForFractional("03-Long", Option(Long.MaxValue.toFloat), Option(Long.MinValue.toDouble)),
      InputRowDoublesForFractional("04-Infinity", Option(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      new InputRowDoublesForFractional("05-Really big", reallyBig),
      InputRowDoublesForFractional("06-NaN", Option(Float.NaN), Option(Double.NaN))
    )
    val src = spark.createDataFrame(seq)
    showDataFrame(src)

    val exp = Seq(
      FractionalRow("01-Pi", Option(Math.PI.toFloat), Option(Math.PI)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-Infinity", Option(0), Option(0),  Seq(
        ErrorMessage.stdCastErr("floatField", "-Infinity"),
        ErrorMessage.stdCastErr("doubleField", "Infinity"))),
      FractionalRow("05-Really big", Option(0), Option(reallyBig), Seq(
        ErrorMessage.stdCastErr("floatField", reallyBig.toString))),
      FractionalRow("06-NaN", Option(0), Option(0), Seq(
        ErrorMessage.stdCastErr("floatField", "NaN"),
        ErrorMessage.stdCastErr("doubleField", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    showDataFrame(std)

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("With infinity from string") {
    val seq = Seq(
      ("01-Euler", "2.71", "2.71"),
      ("02-Null", null, null),
      ("03-Long", Long.MaxValue.toString, Long.MinValue.toString),
      ("04-infinity", "-Infinity", "Infinity"),
      ("05-Really big", "123456789123456791245678912324789123456789123456789.12",
        "-12345678912345679124567891232478912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912346789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123456789123456789.1"),
      ("06-Text", "foo", "bar"),
      ("07-Exponential notation", "-1.23E4", "+9.8765E-3")
    )
    val src = seq.toDF("description","floatField", "doubleField")
    showDataFrame(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithInfinity, "").cache()
    showDataFrame(std)

    val exp = Seq(
      FractionalRow("01-Euler", Option(2.71F), Option(2.71)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-infinity", Some(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      FractionalRow("05-Really big", Option(Float.PositiveInfinity), Option(Double.NegativeInfinity)),
      FractionalRow("06-Text", Option(0), Option(0), Seq(
        ErrorMessage.stdCastErr("floatField", "foo"),
        ErrorMessage.stdCastErr("doubleField", "bar"))),
      FractionalRow("07-Exponential notation", Option(-12300.0f), Option(0.0098765))
    )

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("With infinity from double") {
    val reallyBig = Double.MaxValue
    val seq = Seq(
      new InputRowDoublesForFractional("01-Euler", Math.E),
      InputRowDoublesForFractional("02-Null", None, None),
      InputRowDoublesForFractional("03-Long", Option(Long.MaxValue.toFloat), Option(Long.MinValue.toDouble)),
      InputRowDoublesForFractional("04-Infinity", Option(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      new InputRowDoublesForFractional("05-Really big", reallyBig),
      InputRowDoublesForFractional("06-NaN", Option(Float.NaN), Option(Double.NaN))
    )
    val src = spark.createDataFrame(seq)
    showDataFrame(src)

    val exp = Seq(
      FractionalRow("01-Euler", Option(Math.E.toFloat), Option(Math.E)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-Infinity", Option(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      FractionalRow("05-Really big", Option(Float.PositiveInfinity), Option(reallyBig)),
      FractionalRow("06-NaN", Option(0), Option(0), Seq(
        ErrorMessage.stdCastErr("floatField", "NaN"),
        ErrorMessage.stdCastErr("doubleField", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithInfinity, "").cache()
    showDataFrame(std)

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

}

case class FractionalRow(
                          description: String,
                          floatField: Option[Float],
                          doubleField: Option[Double],
                          errCol: Seq[ErrorMessage] = Seq.empty
                        )

case class InputRowLongsForFractional(
                                       description: String,
                                       floatField: Option[Double],
                                       doubleField: Option[Double]
                                     ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}

case class InputRowDoublesForFractional(
                                         description: String,
                                         floatField: Option[Double],
                                         doubleField: Option[Double]
                                       ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}
