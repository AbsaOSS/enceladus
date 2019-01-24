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

import org.apache.hadoop.hdfs.web.JsonUtil
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.general.JsonUtils

import scala.collection.mutable

// Examples for constructing dataframes containing arrays of various levels of nesting
// Also it includes error column to test transformations that can fail per field

// The case classes were declared at the package level so it can be used to create Spark DataSets
// It is declared package private so the names won't pollute public/exported namespace

// Structs of Structs example
private[transformations] case class Address2(city: String, street: String, buildingNum: Int, zip: String, errors: Seq[ErrorMessage])

private[transformations] case class Employee2(name: String, address: Address2)

private[transformations] case class TestObj11(id: Int, employee: Employee2)

private[transformations] case class TestObj12(id: Int, employee: Seq[Employee2])

class DeepArrayErrorTransformationSuite extends FunSuite with SparkTestBase {

  import spark.implicits._

  implicit val udfLib: UDFLibrary = new UDFLibrary

  // Plain
  val plainSample: Seq[Address2] = Seq(
    Address2("Olomuc", "Vodickova", 12, "12000", Seq()),
    Address2("Ostrava", "Vlavska", 110, "1455a", Seq()),
    Address2("Plzen", "Kralova", 71, "b881",
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value"))))
  )

  // Struct of struct
  val structOfStructSample: Seq[TestObj11] = Seq(
    TestObj11(1, Employee2("Martin", Address2("Olomuc", "Vodickova", 12, "12000", Nil))),
    TestObj11(1, Employee2("Petr", Address2("Ostrava", "Vlavska", 110, "1455a", Nil))),
    TestObj11(1, Employee2("Vojta", Address2("Plzen", "Kralova", 71, "b881", Nil)))
  )

  // Array of struct of struct
  val arrayOfstructOfStructSample: Seq[TestObj12] = Seq(
    TestObj12(1, Seq(Employee2("Martin", Address2("Olomuc", "Vodickova", 732, "73200", Nil)), Employee2("Stephan", Address2("Olomuc", "Vodickova", 77, "77-333", Nil)))),
    TestObj12(2, Seq(Employee2("Petr", Address2("Ostrava", "Vlavska", 25, "a9991", Nil)), Employee2("Michal", Address2("Ostrava", "Vlavska", 334, "552-aa1", Nil)))),
    TestObj12(3, Seq(Employee2("Vojta", Address2("Plzen", "Kralova", 33, "993", Nil))))
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
      //.withColumn("A", concat(col("errors"), col("ErrorOsosOsosOsos")))

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

