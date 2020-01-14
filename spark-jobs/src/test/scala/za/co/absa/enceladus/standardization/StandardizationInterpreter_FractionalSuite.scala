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

package za.co.absa.enceladus.standardization

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class StandardizationInterpreter_FractionalSuite extends FunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  private def err(value: String, cnt: Int): Seq[ErrorMessage] = {
    val item = ErrorMessage.stdCastErr("src",value)
    val array = Array.fill(cnt) (item)
    array.toList
  }

  private val desiredSchema = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("floatField", FloatType, nullable = false),
    StructField("doubleField", DoubleType, nullable = true)
  ))

  private val desiredSchemaWithInfinity = StructType(Seq(
    StructField("description", StringType, nullable = false),
    StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder().putString("allow_infinity", value = "true").build),
    StructField("doubleField", DoubleType, nullable = true,
      new MetadataBuilder().putString("allow_infinity", value = "true").build)
  ))

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
      ("07-Exponential notation", "-1.23E4", "+9.8765E-3")
    )
    val src = seq.toDF("description","floatField", "doubleField")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      FractionalRow("01-Pi", Option(3.14F), Option(3.14)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-infinity", Option(0), None,  Seq(
        ErrorMessage.stdCastErr("floatField", "-Infinity"),
        ErrorMessage.stdCastErr("doubleField", "Infinity"))),
      FractionalRow("05-Really big", Option(0), None, Seq(
        ErrorMessage.stdCastErr("floatField", "123456789123456791245678912324789123456789123456789.12"),
        ErrorMessage.stdCastErr("doubleField", "12345678912345679124567891232478912345678912345678912"
          + "3456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789"
          + "1234567891234567891234567891234567891234567891234678912345678912345678912345678912345679124567891232478912"
          + "3456789123456789123456789123456789123456791245678912324789123456789123456789123456789123456789123456789123"
          + "456789123456789.1"))),
      FractionalRow("06-Text", Option(0), None, Seq(
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
      InputRowLongsForFractional("02-Big Long", Option(Long.MaxValue - 1), Option(Long.MinValue + 1)),
      InputRowLongsForFractional("03-Long", Option(-value), Option(value))
    )
    val src = spark.createDataFrame(seq)
    logDataFrameContent(src)

    val exp = Seq(
      FractionalRow("01-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("02-Big Long", Option(9.223372E18F), Option(-9.223372036854776E18)), //NBN! the loss of precision
      FractionalRow("03-Long", Option(-value.toFloat), Option(value.toDouble))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

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
    logDataFrameContent(src)

    val exp = Seq(
      FractionalRow("01-Pi", Option(Math.PI.toFloat), Option(Math.PI)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-Infinity", Option(0), None,  Seq(
        ErrorMessage.stdCastErr("floatField", "-Infinity"),
        ErrorMessage.stdCastErr("doubleField", "Infinity"))),
      FractionalRow("05-Really big", Option(0), Option(reallyBig), Seq(
        ErrorMessage.stdCastErr("floatField", reallyBig.toString))),
      FractionalRow("06-NaN", Option(0), None, Seq(
        ErrorMessage.stdCastErr("floatField", "NaN"),
        ErrorMessage.stdCastErr("doubleField", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("With infinity from string") {
    val seq = Seq(
      ("01-Euler", "2.71", "2.71"),
      ("02-Null", null, null),
      ("03-Long", Long.MaxValue.toString, Long.MinValue.toString),
      ("04-infinity", "-∞", "∞"),
      ("05-Really big", "123456789123456791245678912324789123456789123456789.12",
        "-1234567891234567912456789123247891234567891234567891234567891234567891234567891234567891234567891234567891234"
        + "567891234567891234567891234567891234567891234567891234567891234567891234567891234567891234567891234678912345"
        + "678912345678912345678912345679124567891232478912345678912345678912345678912345678912345679124567891232478912"
        + "3456789123456789123456789123456789123456789123456789123456789.1"),
      ("06-Text", "foo", "bar"),
      ("07-Exponential notation", "-1.23E4", "+9.8765E-3")
    )
    val src = seq.toDF("description","floatField", "doubleField")
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithInfinity, "").cache()
    logDataFrameContent(std)

    val exp = Seq(
      FractionalRow("01-Euler", Option(2.71F), Option(2.71)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-infinity", Some(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      FractionalRow("05-Really big", Option(Float.PositiveInfinity), Option(Double.NegativeInfinity)),
      FractionalRow("06-Text", Option(0), None, Seq(
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
    logDataFrameContent(src)

    val exp = Seq(
      FractionalRow("01-Euler", Option(Math.E.toFloat), Option(Math.E)),
      FractionalRow("02-Null", Option(0), None, Seq(
        ErrorMessage.stdNullErr("floatField"))),
      FractionalRow("03-Long", Option(9.223372E18F), Option(-9.223372036854776E18)),
      FractionalRow("04-Infinity", Option(Float.NegativeInfinity), Option(Double.PositiveInfinity)),
      FractionalRow("05-Really big", Option(Float.PositiveInfinity), Option(reallyBig)),
      FractionalRow("06-NaN", Option(0), None, Seq(
        ErrorMessage.stdCastErr("floatField", "NaN"),
        ErrorMessage.stdCastErr("doubleField", "NaN")))
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithInfinity, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[FractionalRow].collect().sortBy(_.description).toList)
  }

  test("No pattern, but altered symbols") {
    val input = Seq(
      ("01-Positive", "+3"),
      ("02-Negative", "~8123,4"),
      ("03-Null", null),
      ("04-Big", "7899012345678901234567890123456789012346789,123456789"),
      ("05-Big II", "+1E40"),
      ("06-Big III", "2E308"),
      ("07-Small", "~7899012345678901234567890123456789012346789,123456789"),
      ("08-Small II", "~1,1E40"),
      ("09-Small III", "~3E308"),
      ("10-Wrong", "hello"),
      ("11-Infinity", "+∞"),
      ("12-Negative Infinity", "~∞"),
      ("13-Old decimal", "5.5"),
      ("14-Old minus", "-10"),
      ("15-Infinity as word", "Infinity")
    )

    val src = input.toDF("description", "src")

    val decimalSeparator = ","
    val minusSign = "~"
    val srcField = "src"

    val desiredSchemaWithAlters = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("small", FloatType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("big", DoubleType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.DefaultValue, "+1000,001")
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("small_with_infinity", FloatType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DefaultValue, "~999999,9999")
        .putString(MetadataKeys.AllowInfinity, "True")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build()),
      StructField("big_with_infinity", DoubleType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.AllowInfinity, "True")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.MinusSign, minusSign)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithAlters, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Positive", "+3", 3.0F, Some(3.0D), Some(3.0F), 3.0D, Seq.empty),
      ("02-Negative", "~8123,4", -8123.4F, Some(-8123.4D), Some(-8123.4F), -8123.4D, Seq.empty),
      ("03-Null", null, 0F, None, None, 0D, Array.fill(2)(ErrorMessage.stdNullErr("src")).toList),
      ("04-Big", "7899012345678901234567890123456789012346789,123456789", 0F, Some(7.899012345678901E42D), Some(Float.PositiveInfinity), 7.899012345678901E42,
        err("7899012345678901234567890123456789012346789,123456789", 1)
      ),
      ("05-Big II", "+1E40", 0F, Some(1.0E40D), Some(Float.PositiveInfinity), 1.0E40D, err("+1E40", 1)),
      ("06-Big III", "2E308", 0F, Some(1000.001D), Some(Float.PositiveInfinity), Double.PositiveInfinity, err("2E308", 2)),
      ("07-Small", "~7899012345678901234567890123456789012346789,123456789", 0F, Some(-7.899012345678901E42D), Some(Float.NegativeInfinity), -7.899012345678901E42,
        err("~7899012345678901234567890123456789012346789,123456789", 1)
      ),
      ("08-Small II", "~1,1E40", 0F, Some(-1.1E40D), Some(Float.NegativeInfinity), -1.1E40D, err("~1,1E40", 1)),
      ("09-Small III", "~3E308", 0F, Some(1000.001D), Some(Float.NegativeInfinity), Double.NegativeInfinity, err("~3E308", 2)),
      ("10-Wrong", "hello", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("hello", 4)),
      ("11-Infinity", "+∞", 0F, Some(1000.001D), Some(Float.PositiveInfinity), Double.PositiveInfinity, err("+∞", 2)),
      ("12-Negative Infinity", "~∞", 0F, Some(1000.001D), Some(Float.NegativeInfinity), Double.NegativeInfinity, err("~∞", 2)),
      ("13-Old decimal", "5.5", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("5.5", 4)),
      ("14-Old minus", "-10", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("-10", 4)),
      ("15-Infinity as word", "Infinity", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("Infinity", 4))
    )
    assertResult(exp)(std.as[(String, String, Float, Option[Double], Option[Float], Double, Seq[ErrorMessage])].collect().toList)
  }

  test("Using patterns") {
    val input = Seq(
      ("01-Positive", "+3°"),
      ("02-Negative", "(8 123,4°)"),
      ("03-Null", null),
      ("04-Big", "+789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°"),
      ("05-Big II", "+1E40°"),
      ("06-Big III", "+2E308°"),
      ("07-Small", "(789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°)"),
      ("08-Small II", "(1,1E40°)"),
      ("09-Small III", "(3E308°)"),
      ("10-Wrong", "hello"),
      ("11-Not adhering to pattern", "(1 234,56)"),
      ("12-Not adhering to pattern II","+1,234.56°"),
      ("13-Infinity", "+∞°"),
      ("14-Negative Infinity", "(∞°)")
    )

    val src = input.toDF("description", "src")

    val pattern = "+#,000.#°;(#,000.#°)"
    val decimalSeparator = ","
    val groupingSeparator = " "
    val srcField = "src"

    val desiredSchemaWithPatterns = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("src", StringType, nullable = true),
      StructField("small", FloatType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .build()),
      StructField("big", DoubleType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.DefaultValue, "+1 000,001°")
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .build()),
      StructField("small_with_infinity", FloatType, nullable = true, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.DefaultValue, "(999 999,9999°)")
        .putString(MetadataKeys.AllowInfinity, "True")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .build()),
      StructField("big_with_infinity", DoubleType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.Pattern, pattern)
        .putString(MetadataKeys.SourceColumn, srcField)
        .putString(MetadataKeys.AllowInfinity, "True")
        .putString(MetadataKeys.DecimalSeparator, decimalSeparator)
        .putString(MetadataKeys.GroupingSeparator, groupingSeparator)
        .build())
    ))

    val std = StandardizationInterpreter.standardize(src, desiredSchemaWithPatterns, "").cache()
    logDataFrameContent(std)

    val exp = List(
      ("01-Positive", "+3°", 3.0F, Some(3.0D), Some(3.0F), 3.0D, Seq.empty),
      ("02-Negative", "(8 123,4°)", -8123.4F, Some(-8123.4D), Some(-8123.4F), -8123.4D, Seq.empty),
      ("03-Null", null, 0F, None, None, 0D, Array.fill(2)(ErrorMessage.stdNullErr("src")).toList),
      ("04-Big", "+789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°", 0F, Some(7.899012345678901E42D), Some(Float.PositiveInfinity), 7.899012345678901E42,
        err("+789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°", 1)
      ),
      ("05-Big II", "+1E40°", 0F, Some(1.0E40D), Some(Float.PositiveInfinity), 1.0E40D, err("+1E40°", 1)),
      ("06-Big III", "+2E308°", 0F, Some(1000.001D), Some(Float.PositiveInfinity), Double.PositiveInfinity, err("+2E308°", 2)),
      ("07-Small", "(789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°)", 0F, Some(-7.899012345678901E42D), Some(Float.NegativeInfinity), -7.899012345678901E42,
        err("(789 9012 345 678 901 234 567 890 123 456 789 012 346 789,123456789°)", 1)
      ),
      ("08-Small II", "(1,1E40°)", 0F, Some(-1.1E40D), Some(Float.NegativeInfinity), -1.1E40D, err("(1,1E40°)", 1)),
      ("09-Small III", "(3E308°)", 0F, Some(1000.001D), Some(Float.NegativeInfinity), Double.NegativeInfinity, err("(3E308°)", 2)),
      ("10-Wrong", "hello", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("hello", 4)),
      ("11-Not adhering to pattern", "(1 234,56)", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("(1 234,56)", 4)),
      ("12-Not adhering to pattern II","+1,234.56°", 0F, Some(1000.001D), Some(-1000000.0F), 0D, err("+1,234.56°", 4)),
      ("13-Infinity", "+∞°", 0F, Some(1000.001D), Some(Float.PositiveInfinity), Double.PositiveInfinity, err("+∞°", 2)),
      ("14-Negative Infinity", "(∞°)", 0F, Some(1000.001D), Some(Float.NegativeInfinity), Double.NegativeInfinity, err("(∞°)", 2))
    )

    assertResult(exp)(std.as[(String, String, Float, Option[Double], Option[Float], Double, Seq[ErrorMessage])].collect().toList)
  }
}
