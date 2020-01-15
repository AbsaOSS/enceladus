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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

case class ErrorPreserve(a: String, b: String, errCol: List[ErrorMessage])
case class ErrorPreserveStd(a: String, b: Int, errCol: List[ErrorMessage])

case class MyWrapper(counterparty: MyHolder)
case class MyHolder(yourRef: String)
case class MyWrapperStd(counterparty: MyHolder, errCol: Seq[ErrorMessage])

case class Time(id: Int, date: String, timestamp: String)
case class StdTime(id: Int, date: Date, timestamp: Timestamp, errCol: List[ErrorMessage])

class StdInterpreterSuite extends FunSuite with SparkTestBase with LoggerTestBase {
import spark.implicits._

  case class subCC(subFieldA: Integer, subFieldB: String)
  case class sub2CC(subSub2FieldA: Integer, subSub2FieldB: String)
  case class sub1CC(subStruct2: sub2CC)
  case class subarrayCC(arrayFieldA: Integer, arrayFieldB: String, arrayStruct: subCC)
  case class rootCC(rootField: String, rootStruct: subCC, rootStruct2: sub1CC, rootArray: Array[subarrayCC])

  val stdExpectedSchema = StructType(
    Seq(
      StructField("rootField", StringType, nullable = true),
      StructField("rootStruct",
        StructType(
          Seq(
            StructField("subFieldA", IntegerType, nullable = true),
            StructField("subFieldB", StringType, nullable = true))), nullable = false),
      StructField("rootStruct2",
        StructType(
          Seq(
            StructField("subStruct2",
              StructType(
                Seq(
                  StructField("subSub2FieldA", IntegerType, nullable = true),
                  StructField("subSub2FieldB", StringType, nullable = true))), nullable = false))), nullable = false),
      StructField("rootArray",
        ArrayType(
          StructType(
            Seq(
              StructField("arrayFieldA", IntegerType, nullable = true),
              StructField("arrayFieldB", StringType, nullable = true),
              StructField("arrayStruct",
                StructType(
                  Seq(
                    StructField("subFieldA", IntegerType, nullable = true),
                    StructField("subFieldB", StringType, nullable = true))), nullable = false))), containsNull = false
        ))))

  test("Non-null errors produced for non-nullable attribute in a struct") {
    import spark.implicits._
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val orig = spark.createDataFrame(Seq(
      MyWrapper(MyHolder(null)),
      MyWrapper(MyHolder("447129"))))

    val exp = Seq(
      MyWrapperStd(MyHolder(""), Seq(ErrorMessage.stdNullErr("counterparty.yourRef"))),
      MyWrapperStd(MyHolder("447129"), Seq()))

    val schema = StructType(Seq(
      StructField("counterparty", StructType(
        Seq(
          StructField("yourRef", StringType, nullable = false))), nullable = false)))

    val standardizedDF = StandardizationInterpreter.standardize(orig, schema, "")

    assertResult(exp)(standardizedDF.as[MyWrapperStd].collect().toList)
  }

  test("Existing error messages should be preserved") {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    import spark.implicits._

    val df = spark.createDataFrame(Array(
      new ErrorPreserve("a", "1", null),
      new ErrorPreserve("b", "2", List()),
      new ErrorPreserve("c", "3", List(new ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
      new ErrorPreserve("d", "abc", List(new ErrorMessage("myErrorType2", "E-2", "Testing This stuff blabla", "whatEvColumn2", Seq("some other value"))))))

    val exp = Array(
      new ErrorPreserveStd("a", 1, List()),
      new ErrorPreserveStd("b", 2, List()),
      new ErrorPreserveStd("c", 3, List(new ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
      new ErrorPreserveStd("d", 0, List(ErrorMessage.stdCastErr("b", "abc"),
        new ErrorMessage("myErrorType2", "E-2", "Testing This stuff blabla", "whatEvColumn2", Seq("some other value")))))

    val expSchema = spark.emptyDataset[ErrorPreserveStd].schema
    val res = StandardizationInterpreter.standardize(df, expSchema, "")

    assertResult(exp.sortBy(_.a).toList)(res.as[ErrorPreserveStd].collect().sortBy(_.a).toList)
  }

  test("Standardize Test") {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val sourceDF = spark.createDataFrame(
      Array(
        rootCC("rootfieldval",
          subCC(123, "subfieldval"),
          sub1CC(sub2CC(456, "subsubfieldval")),
          Array(subarrayCC(789, "arrayfieldval", subCC(321, "xyz"))))))

    val expectedSchema = stdExpectedSchema.add(
      StructField("errCol",
        ArrayType(
          ErrorMessage.errorColSchema, containsNull = false)))

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, stdExpectedSchema, "")

    logger.debug(standardizedDF.schema.treeString)
    logger.debug(expectedSchema.treeString)

    assert(standardizedDF.schema.treeString === expectedSchema.treeString)
  }

  test("Standardize Test (JSON source)") {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val sourceDF = spark.read.json("src/test/resources/data/standardizeJsonSrc.json")

    val expectedSchema = stdExpectedSchema.add(
      StructField("errCol",
        ArrayType(
          ErrorMessage.errorColSchema, containsNull = false)))

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, stdExpectedSchema, "")

    logger.debug(standardizedDF.schema.treeString)
    logger.debug(expectedSchema.treeString)

    assert(standardizedDF.schema.treeString === expectedSchema.treeString)
  }

  case class OrderCC(orderName: String, deliverName: Option[String])
  case class RootRecordCC(id: Long, name: Option[String], orders: Option[Array[OrderCC]])

  test("Test standardization of non-nullable field of a contains null array") {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("orders", ArrayType(StructType(Array(
          StructField("ordername", StringType, nullable = false),
          StructField("delivername", StringType, nullable = true))), containsNull = true), nullable = true)))

    val sourceDF = spark.createDataFrame(
      Array(
        RootRecordCC(1, Some("Test Name 1"), Some(Array(OrderCC("Order Test Name 1", Some("Deliver Test Name 1"))))),
        RootRecordCC(2, Some("Test Name 2"), Some(Array(OrderCC("Order Test Name 2", None)))),
        RootRecordCC(3, Some("Test Name 3"), None),
        RootRecordCC(4, None, None)))

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, schema, "")

    // 'orders' array is nullable, so it can be omitted
    // But orders[].ordername is not nullable, so it must be specified
    // But absence of orders should not cause validation errors
    val count = standardizedDF.where(size(col("errCol")) > 0).count()

    assert(count == 0)
  }

  test ("Test standardization of Date and Timestamp fields with default value and pattern")
  {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"default": "20250101", "pattern": "yyyyMMdd"}""")),
        StructField("timestamp", TimestampType, nullable = true, Metadata.fromJson("""{"default": "20250101.142626", "pattern": "yyyyMMdd.HHmmss"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "20171004", "20171004.111111"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(1735689600000L), new Timestamp(1735741586000L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, schema, "")
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields with default value, without pattern")
  {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"default": "2025-01-01"}""")),
        StructField("timestamp", TimestampType, nullable = true, Metadata.fromJson("""{"default": "2025-01-01 14:26:26"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "2017-10-04", "2017-10-04 11:11:11"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(1735689600000L), new Timestamp(1735741586000L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, schema, "")
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields without default value, with pattern")
  {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"pattern": "yyyyMMdd"}""")),
        StructField("timestamp", TimestampType, nullable = false, Metadata.fromJson("""{"pattern": "yyyyMMdd.HHmmss"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "20171004", "20171004.111111"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, null, new Timestamp(0L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, schema, "")
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields without default value, without pattern")
  {
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = false),
        StructField("timestamp", TimestampType, nullable = true)))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "2017-10-04", "2017-10-04 11:11:11"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(0L), null, List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = StandardizationInterpreter.standardize(sourceDF, schema, "")
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

}
