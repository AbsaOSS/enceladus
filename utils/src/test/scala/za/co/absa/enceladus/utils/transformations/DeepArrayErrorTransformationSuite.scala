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

package za.co.absa.enceladus.utils.transformations

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.general.JsonUtils

// Examples for constructing dataframes containing arrays of various levels of nesting
// Also it includes error column to test transformations that can fail per field

// The case classes were declared at the package level so it can be used to create Spark DataSets
// It is declared package private so the names won't pollute public/exported namespace

// Structs of Structs example
private[transformations] case class AddressWithErrColumn(city: String, street: String, buildingNum: Int, zip: String, errors: Seq[ErrorMessage])

private[transformations] case class AddressNoErrColumn(city: String, street: String, buildingNum: Int, zip: String)

private[transformations] case class EmployeeNoErrorColumn(name: String, address: AddressNoErrColumn)

private[transformations] case class TestObj1WithErrorColumn(id: Int, employee: EmployeeNoErrorColumn, errors: Seq[ErrorMessage])

private[transformations] case class TestObj2WithErrorColumn(id: Int, employee: Seq[EmployeeNoErrorColumn], errors: Seq[ErrorMessage])

// Arrays of primitives example
private[transformations] case class FunNumbersWithErrorColumn(id: Int, nums: Seq[String], errors: Seq[ErrorMessage])

// Arrays of arrays of primitives example
private[transformations] case class GeoDataWithErrorColumn(id: Int, matrix: Seq[Seq[String]], errors: Seq[ErrorMessage])

class DeepArrayErrorTransformationSuite extends FunSuite with SparkTestBase {

  import spark.implicits._

  implicit val udfLib: UDFLibrary = new UDFLibrary

  // Plain
  val plainSample: Seq[AddressWithErrColumn] = Seq(
    AddressWithErrColumn("Olomuc", "Vodickova", 12, "12000", Seq()),
    AddressWithErrColumn("Ostrava", "Vlavska", 110, "1455a", Seq()),
    AddressWithErrColumn("Plzen", "Kralova", 71, "b881",
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value"))))
  )

  // Struct of struct
  val structOfStructSample: Seq[TestObj1WithErrorColumn] = Seq(
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Martin", AddressNoErrColumn("Olomuc", "Vodickova", 12, "12000")), Nil),
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Petr", AddressNoErrColumn("Ostrava", "Vlavska", 110, "1455a")),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Vojta", AddressNoErrColumn("Plzen", "Kralova", 71, "b881")), Nil)
  )

  // Array of struct of struct
  val arrayOfstructOfStructSample: Seq[TestObj2WithErrorColumn] = Seq(
    TestObj2WithErrorColumn(1, Seq(
      EmployeeNoErrorColumn("Martin", AddressNoErrColumn("Olomuc", "Vodickova", 732, "73200")),
      EmployeeNoErrorColumn("Stephan", AddressNoErrColumn("Olomuc", "Vodickova", 77, "77-333"))), Nil),
    TestObj2WithErrorColumn(2, Seq(
      EmployeeNoErrorColumn("Petr", AddressNoErrColumn("Ostrava", "Vlavska", 25, "a9991")),
      EmployeeNoErrorColumn("Michal", AddressNoErrColumn("Ostrava", "Vlavska", 334, "552-aa1"))),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    TestObj2WithErrorColumn(3, Seq(
      EmployeeNoErrorColumn("Vojta", AddressNoErrColumn("Plzen", "Kralova", 33, "993"))), Nil)
  )

  // Arrays of primitives
  val arraysOfPrimitivesSample: Seq[FunNumbersWithErrorColumn] = Seq(
    FunNumbersWithErrorColumn(1,Seq("7755", "a212", "222-111"),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    FunNumbersWithErrorColumn(1,Seq("223a", "223a", "775"), Nil),
    FunNumbersWithErrorColumn(1,Seq("5", "-100", "9999999"), Nil)
  )

  // Arrays of arrays of primitives
  val arraysOfArraysOfPrimitivesSample: Seq[GeoDataWithErrorColumn] = Seq(
    GeoDataWithErrorColumn(1,Seq(Seq("10", "11b"), Seq("11b", "12")),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    GeoDataWithErrorColumn(2,Seq(Seq("20f", "300"), Seq("1000", "10-10")), Nil),
    GeoDataWithErrorColumn(3,Seq(Seq("775", "223"), Seq("100", "0")), Nil)
  )

  test("Test casting of a plain field with error column") {
    val df = spark.sparkContext.parallelize(plainSample).toDF

    val dfOut = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, "zip", "intZip", "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit("zip"), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults =  JsonUtils.prettySparkJSON(dfOut.toJSON.collect.mkString("\n"))

    dfOut.explain(true)

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        | |-- street: string (nullable = true)
        | |-- buildingNum: integer (nullable = false)
        | |-- zip: string (nullable = true)
        | |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "city" : "Olomuc",
        |  "street" : "Vodickova",
        |  "buildingNum" : 12,
        |  "zip" : "12000",
        |  "intZip" : 12000,
        |  "errors" : [ null ]
        |}, {
        |  "city" : "Ostrava",
        |  "street" : "Vlavska",
        |  "buildingNum" : 110,
        |  "zip" : "1455a",
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "zip",
        |    "rawValues" : [ "1455a" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "city" : "Plzen",
        |  "street" : "Kralova",
        |  "buildingNum" : 71,
        |  "zip" : "b881",
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "zip",
        |    "rawValues" : [ "b881" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    dfOut.printSchema()
    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test casting of a struct of struct field with error column") {
    val df = spark.sparkContext.parallelize(structOfStructSample).toDF

    val dfOut = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, "employee.address.zip", "employee.address.intZip", "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit("employee.address.zip"), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults =  JsonUtils.prettySparkJSON(dfOut.toJSON.collect.mkString("\n"))

    dfOut.explain(true)

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- city: string (nullable = true)
        | |    |    |-- street: string (nullable = true)
        | |    |    |-- buildingNum: integer (nullable = true)
        | |    |    |-- zip: string (nullable = true)
        | |    |    |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Martin",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 12,
        |      "zip" : "12000",
        |      "intZip" : 12000
        |    }
        |  },
        |  "errors" : [ null ]
        |}, {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Petr",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 110,
        |      "zip" : "1455a"
        |    }
        |  },
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.zip",
        |    "rawValues" : [ "1455a" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Vojta",
        |    "address" : {
        |      "city" : "Plzen",
        |      "street" : "Kralova",
        |      "buildingNum" : 71,
        |      "zip" : "b881"
        |    }
        |  },
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.zip",
        |    "rawValues" : [ "b881" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    dfOut.printSchema()
    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test casting of an array of struct of struct with error column") {
    val df = spark.sparkContext.parallelize(arrayOfstructOfStructSample).toDF

    val dfOut = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, "employee.address.zip", "employee.address.intZip", "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit("employee.address.zip"), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults =  JsonUtils.prettySparkJSON(dfOut.toJSON.collect.mkString("\n"))

    dfOut.explain(true)

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- name: string (nullable = true)
        | |    |    |-- address: struct (nullable = false)
        | |    |    |    |-- city: string (nullable = true)
        | |    |    |    |-- street: string (nullable = true)
        | |    |    |    |-- buildingNum: integer (nullable = true)
        | |    |    |    |-- zip: string (nullable = true)
        | |    |    |    |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "employee" : [ {
        |    "name" : "Martin",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 732,
        |      "zip" : "73200",
        |      "intZip" : 73200
        |    }
        |  }, {
        |    "name" : "Stephan",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 77,
        |      "zip" : "77-333"
        |    }
        |  } ],
        |  "errors" : [ null, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.zip",
        |    "rawValues" : [ "77-333" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "employee" : [ {
        |    "name" : "Petr",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 25,
        |      "zip" : "a9991"
        |    }
        |  }, {
        |    "name" : "Michal",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 334,
        |      "zip" : "552-aa1"
        |    }
        |  } ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.zip",
        |    "rawValues" : [ "a9991" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.zip",
        |    "rawValues" : [ "552-aa1" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 3,
        |  "employee" : [ {
        |    "name" : "Vojta",
        |    "address" : {
        |      "city" : "Plzen",
        |      "street" : "Kralova",
        |      "buildingNum" : 33,
        |      "zip" : "993",
        |      "intZip" : 993
        |    }
        |  } ],
        |  "errors" : [ null ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    dfOut.printSchema()
    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test casting of an array of primitives") {
    val df = spark.sparkContext.parallelize(arraysOfPrimitivesSample).toDF

    val dfOut = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, "nums", "intNums", "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit("nums"), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults =  JsonUtils.prettySparkJSON(dfOut.toJSON.collect.mkString("\n"))

    dfOut.explain(true)

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- nums: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        | |-- intNums: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "nums" : [ "7755", "a212", "222-111" ],
        |  "intNums" : [ 7755, null, null ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, null, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "nums",
        |    "rawValues" : [ "a212" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "nums",
        |    "rawValues" : [ "222-111" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 1,
        |  "nums" : [ "223a", "223a", "775" ],
        |  "intNums" : [ null, null, 775 ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "nums",
        |    "rawValues" : [ "223a" ],
        |    "mappings" : [ ]
        |  }, null ]
        |}, {
        |  "id" : 1,
        |  "nums" : [ "5", "-100", "9999999" ],
        |  "intNums" : [ 5, -100, 9999999 ],
        |  "errors" : [ null ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    dfOut.printSchema()
    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test casting of an array of array of primitives") {
    val df = spark.sparkContext.parallelize(arraysOfArraysOfPrimitivesSample).toDF

    val dfOut = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, "matrix", "intMatrix", "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit("matrix"), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults =  JsonUtils.prettySparkJSON(dfOut.toJSON.collect.mkString("\n"))

    dfOut.explain(true)

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- matrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: string (containsNull = true)
        | |-- intMatrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: integer (containsNull = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "matrix" : [ [ "10", "11b" ], [ "11b", "12" ] ],
        |  "intMatrix" : [ [ 10, null ], [ null, 12 ] ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, null, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "matrix",
        |    "rawValues" : [ "11b" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "matrix" : [ [ "20f", "300" ], [ "1000", "10-10" ] ],
        |  "intMatrix" : [ [ null, 300 ], [ 1000, null ] ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "matrix",
        |    "rawValues" : [ "20f" ],
        |    "mappings" : [ ]
        |  }, null, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "matrix",
        |    "rawValues" : [ "10-10" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 3,
        |  "matrix" : [ [ "775", "223" ], [ "100", "0" ] ],
        |  "intMatrix" : [ [ 775, 223 ], [ 100, 0 ] ],
        |  "errors" : [ null ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    dfOut.printSchema()
    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      println("EXPECTED:")
      println(expectedSchema)
      println("ACTUAL:")
      println(actualSchema)
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  private def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (actualResults != expectedResults) {
      println("EXPECTED:")
      println(expectedResults)
      println("ACTUAL:")
      println(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }
}

