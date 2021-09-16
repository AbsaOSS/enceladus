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

package za.co.absa.enceladus.standardization.interpreter

import java.sql.Date
import org.apache.spark.sql.types.{DateType, MetadataBuilder, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationInterpreter_DateSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary
  private implicit val defaults: Defaults = GlobalDefaults

  private val fieldName = "dateField"

  test("epoch") {
    val seq  = Seq(
      0,
      86399,
      86400,
      978307199,
      1563288103
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "epoch").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochmilli") {
    val seq  = Seq(
      0L,
      86400000,
      978307199999L,
      1563288103123L
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "epochmilli").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochmicro") {
    val seq  = Seq(
      0.1,
      86400000000.02,
      978307199999999.003,
      1563288103123456.123
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "epochmicro").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochnano") {
    val seq  = Seq(
      0,
      86400000000000L,
      978307199999999999L,
      1563288103123456789L
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "epochnano").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("simple date pattern") {
    val seq  = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "yyyy/dd/MM").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "1970-02-02"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date + time pattern and named time zone") {
    val seq  = Seq(
      "01-00-00 01.01.1970 CET",
      "00-00-00 03.01.1970 EET",
      "21-45-39 30.12.2000 PST",
      "14-25-11 16.07.2019 UTC",
      "00-75-00 03.01.1970 EET",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "HH-mm-ss dd.MM.yyyy ZZZ").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "00-75-00 03.01.1970 EET"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date + time + second fractions pattern and offset time zone") {
    val seq  = Seq(
      "01:00:00(000000000) 01+01+1970 +01:00",
      "00:00:00(001002003) 03+01+1970 +02:00",
      "21:45:39(999999999) 30+12+2000 -08:00",
      "14:25:11(123456789) 16+07+2019 +00:00",
      "00:75:00(001002003) 03+01+1970 +02:00",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "HH:mm:ss(SSSnnnnnn) dd+MM+yyyy XXX").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "00:75:00(001002003) 03+01+1970 +02:00"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date with  default time zone - EST") {
    val seq  = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "yyyy/dd/MM")
          .putString("timezone", "EST")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "1970-02-02"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }


  test("date with  default time zone - SAST") {
    val seq  = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "yyyy/dd/MM")
          .putString("timezone", "Africa/Johannesburg")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1969-12-31")),
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("2000-12-30")),
      DateRow(Date.valueOf("2019-07-15")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "1970-02-02"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date with quoted") {
    val seq  = Seq(
      "January 1 of 1970",
      "February 1 of 1970",
      "December 31 of 2000",
      "July 16 of 2019",
      "02 3 of 1970",
      "February 4 1970",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString("pattern", "MMMM d 'of' yyyy").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-02-01")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "02 3 of 1970"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "February 4 1970"))),
      DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  /* TODO this should work with #677 fixed
  test("date with quoted and second frations") {
  val seq  = Seq(
    "1970/01/01 insignificant 000000",
    "1970/02/01 insignificant 001002",
    "2000/31/12 insignificant 999999",
    "2019/16/07 insignificant 123456",
    "1970/02/02 insignificant ",
    "crash"
  )
  val desiredSchema = StructType(Seq(
    StructField(fieldName, DateType, nullable = false,
      new MetadataBuilder().putString("pattern", "yyyy/MM/dd 'insignificant' iiiiii").build)
  ))
  val exp = Seq(
    DateRow(Date.valueOf("1970-01-01")),
    DateRow(Date.valueOf("1970-02-01")),
    DateRow(Date.valueOf("2000-12-31")),
    DateRow(Date.valueOf("2019-07-16")),
    DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "1970/02/02 insignificant "))),
    DateRow(Date.valueOf("1970-01-01"), Seq(ErrorMessage.stdCastErr(fieldName, "crash")))
  )

  val src = seq.toDF(fieldName)

  val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
  logDataFrameContent(std)

  assertResult(exp)(std.as[DateRow].collect().toList)
  }
  */

}
