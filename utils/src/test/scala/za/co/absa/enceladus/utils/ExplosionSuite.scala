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

package za.co.absa.enceladus.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.utils.explode.ExplodeTools
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils.RowSeqToDf
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.spark.hats.Extensions._

class ExplosionSuite extends AnyFunSuite with SparkTestBase with DatasetComparer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  import spark.implicits._

  test("Test explosion of a simple array") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val df = sampleArray.toDF()

    val expectedSchema = """root
                           | |-- value: integer (nullable = true)
                           | |-- value_id: long (nullable = false)
                           | |-- value_size: integer (nullable = false)
                           | |-- value_idx: integer (nullable = true)
                           |""".stripMargin.replace("\r\n", "\n")
    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", df)

    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema.treeString, expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(1, 0L, 10, 0),
      Row(2, 0L, 10, 1),
      Row(3, 0L, 10, 2),
      Row(4, 0L, 10, 3),
      Row(5, 0L, 10, 4),
      Row(6, 0L, 10, 5),
      Row(7, 0L, 10, 6),
      Row(8, 0L, 10, 7),
      Row(9, 0L, 10, 8),
      Row(10, 0L, 10, 9),
      Row(2, 1L, 10, 0),
      Row(3, 1L, 10, 1),
      Row(4, 1L, 10, 2),
      Row(5, 1L, 10, 3),
      Row(6, 1L, 10, 4),
      Row(7, 1L, 10, 5),
      Row(8, 1L, 10, 6),
      Row(9, 1L, 10, 7),
      Row(10, 1L, 10, 8),
      Row(11, 1L, 10, 9)
    )
    val expectedDf = expectedData.toDfWithSchema(explodedDf.schema)
    assertSmallDatasetEquality(explodedDf.limit(20), expectedDf) // checking just the data: just 20 first rows
  }

  test("Test a simple array reconstruction") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val df = sampleArray.toDF().withColumn("static", lit(1))

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", df)

    // Checking if explosion has been done correctly - schema
    val expectedExplodedSchema =
      """root
        | |-- value: integer (nullable = true)
        | |-- static: integer (nullable = false)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema.treeString, expectedExplodedSchema) // checking schema first

    // Checking if explosion has been done correctly - data
    val expectedExplodedData = Seq(
      Row(1, 1, 0L, 10, 0),
      Row(2, 1, 0L, 10, 1),
      Row(3, 1, 0L, 10, 2),
      Row(4, 1, 0L, 10, 3),
      Row(5, 1, 0L, 10, 4),
      Row(6, 1, 0L, 10, 5),
      Row(7, 1, 0L, 10, 6),
      Row(8, 1, 0L, 10, 7),
      Row(9, 1, 0L, 10, 8),
      Row(10, 1, 0L, 10, 9),
      Row(2, 1, 1L, 10, 0),
      Row(3, 1, 1L, 10, 1),
      Row(4, 1, 1L, 10, 2),
      Row(5, 1, 1L, 10, 3),
      Row(6, 1, 1L, 10, 4),
      Row(7, 1, 1L, 10, 5),
      Row(8, 1, 1L, 10, 6),
      Row(9, 1, 1L, 10, 7),
      Row(10, 1, 1L, 10, 8),
      Row(11, 1, 1L, 10, 9)
    )
    val expectedExplodedDf = expectedExplodedData.toDfWithSchema(explodedDf.schema)
    assertSmallDatasetEquality(explodedDf.limit(20), expectedExplodedDf) // checking just the data: just 20 first rows

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if restoration has been done correctly - schema
    val expectedRestoredSchema =
      """root
        | |-- static: integer (nullable = false)
        | |-- value: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
      Row(1, Seq(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)),
      Row(1, Seq(3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
      Row(1, Seq(4, 5, 6, 7, 8, 9, 10, 11, 12, 13)),
      Row(1, Seq(5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
    )

    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test("Test a array of array sequence of explosions") {
    // Example provided by Saša Zejnilović
    val sampleMatrix = List(
      List(
        List(1, 2, 3, 4, 5, 6),
        List(7, 8, 9, 10, 11, 12, 13)
      ), List(
        List(201, 202, 203, 204, 205, 206),
        List(207, 208, 209, 210, 211, 212, 213)
      ), List(
        List(301, 302, 303, 304, 305, 306),
        List(307, 308, 309, 310, 311, 312, 313)
      ), List(
        List(401, 402, 403, 404, 405, 406),
        List(407, 408, 409, 410, 411, 412, 413)
      )
    )
    val df = sampleMatrix.toDF().withColumn("static", lit(1))

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("value", df)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("value", explodedDf1, explodeContext1)

    // Checking if explosion has been done correctly - schema
    val expectedExplodedSchema =
      """root
        | |-- value: integer (nullable = true)
        | |-- static: integer (nullable = false)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        | |-- value_id_1: long (nullable = false)
        | |-- value_size_1: integer (nullable = false)
        | |-- value_idx_1: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assert(explodeContext2.explosions.size == 2)
    assertSchema(explodedDf2.schema.treeString, expectedExplodedSchema)

    // Checking if explosion has been done correctly - data
    val expectedExplodedData = Seq(
      Row(1, 1, 0L, 2, 0, 0L, 6, 0),
      Row(2, 1, 0L, 2, 0, 0L, 6, 1),
      Row(3, 1, 0L, 2, 0, 0L, 6, 2),
      Row(4, 1, 0L, 2, 0, 0L, 6, 3),
      Row(5, 1, 0L, 2, 0, 0L, 6, 4),
      Row(6, 1, 0L, 2, 0, 0L, 6, 5),
      Row(7, 1, 0L, 2, 1, 1L, 7, 0),
      Row(8, 1, 0L, 2, 1, 1L, 7, 1),
      Row(9, 1, 0L, 2, 1, 1L, 7, 2),
      Row(10, 1, 0L, 2, 1, 1L, 7, 3)
    )
    val expectedExplodedDf = expectedExplodedData.toDfWithSchema(explodedDf2.schema)
    assertSmallDatasetEquality(explodedDf2.limit(10), expectedExplodedDf) // checking just the data: just 10 first rows

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf2, explodeContext2)

    // Checking if restoration has been done correctly - schema
    val expectedRestoredSchema =
      """root
        | |-- static: integer (nullable = false)
        | |-- value: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: integer (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1, Seq(Seq(1, 2, 3, 4, 5, 6), Seq(7, 8, 9, 10, 11, 12, 13))),
      Row(1, Seq(Seq(201, 202, 203, 204, 205, 206), Seq(207, 208, 209, 210, 211, 212, 213))),
      Row(1, Seq(Seq(301, 302, 303, 304, 305, 306), Seq(307, 308, 309, 310, 311, 312, 313))),
      Row(1, Seq(Seq(401, 402, 403, 404, 405, 406), Seq(407, 408, 409, 410, 411, 412, 413)))
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test("Test handling of empty and null arrays") {
    val sample = Seq("""{"value":[1,2,3,4,5,6,7,8,9,10],"static":1}""",
      """{"value":[2,3,4,5,6,7,8,9,10,11],"static":2}""",
      """{"value":[],"static":3}""",
      """{"static":4}""")
    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("value", df)

    // Checking if explosion has been done correctly - schema
    val expectedExplodedSchema =
      """root
        | |-- static: long (nullable = true)
        | |-- value: long (nullable = true)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(explodedDf.schema.treeString, expectedExplodedSchema)

    // Checking if explosion has been done correctly - data
    val explodedDfProjection = explodedDf
      .select($"static", $"value_size", $"value_idx", $"value")
      .orderBy($"value_size", $"value_idx", $"static")
    val expectedExplodedData = Seq(
      Row(4L, -1, null, null),
      Row(3L, 0, null, null),
      Row(1L, 10, 0, 1L),
      Row(2L, 10, 0, 2L),
      Row(1L, 10, 1, 2L)
    )
    val expectedExplodedDf = expectedExplodedData.toDfWithSchema(explodedDfProjection.schema)
    assertSmallDatasetEquality(explodedDfProjection.limit(5), expectedExplodedDf) // checking just the data: just 5 first rows

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if restoration has been done correctly - schema
    val expectedRestoredSchema =
      """root
        | |-- static: long (nullable = true)
        | |-- value: array (nullable = true)
        | |    |-- element: long (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1L, Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
      Row(2L, Seq(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L)),
      Row(3L, Seq()),
      Row(4L, null)
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test("Test deconstruct()") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val d = ExplodeTools.deconstructNestedColumn(df, "leg.conditions")
    val (deconstructedDf, deconstructedCol, transientCol) = ExplodeTools.DeconstructedNestedField.unapply(d).get

    // Checking if explosion has been done correctly - schema
    val expectedDeconstructedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- quark: integer (nullable = false)
        | |    |-- legid: long (nullable = true)
        | |-- electron: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- action: string (nullable = true)
        | |    |    |-- check: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(deconstructedDf.schema.treeString, expectedDeconstructedSchema)

    // Checking if explosion has been done correctly - data
    val expectedDeconstructedData = Seq(
      Row(1L, Row(0, 100L), Seq(Row("b", "a"), Row("d", "c"), Row("f", "e"))),
      Row(2L, Row(0, 200L), Seq(Row("h", "g"), Row("j", "i"), Row("l", "k"))),
      Row(3L, Row(0, 300L), Seq()),
      Row(4L, Row(0, 400L), null)
    )
    val expectedDeconstructedDf = expectedDeconstructedData.toDfWithSchema(deconstructedDf.schema)
    assertSmallDatasetEquality(deconstructedDf, expectedDeconstructedDf) // checking just the data

    val restoredDf = ExplodeTools.nestedRenameReplace(deconstructedDf, deconstructedCol, "leg.conditions", transientCol)

    // Checking if restoration has been done correctly - schema
    val expectedRestoredSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        | |    |-- legid: long (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1L, Row(Seq(Row("b", "a"), Row("d", "c"), Row("f", "e")), 100L)),
      Row(2L, Row(Seq(Row("h", "g"), Row("j", "i"), Row("l", "k")), 200L)),
      Row(3L, Row(Seq(), 300L)),
      Row(4L, Row(null, 400L))
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test ("Test multiple nesting of arrays and structs") {
    val sample = """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}""" ::
      """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"amount":200}]}]}""" ::
      """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"amount": 300}]}]}""" ::
      """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"amount": 400}]}]}""" ::
      """{"id":5,"legs":[{"legid":500,"conditions":[]}]}""" ::
      """{"id":6,"legs":[]}""" ::
      """{"id":7}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    // Checking that the original data is correct - schema
    assert(SchemaUtils.isNonNestedArray(df.schema, "legs"))
    assert(!SchemaUtils.isNonNestedArray(df.schema, "legs.conditions"))
    assert(!SchemaUtils.isNonNestedArray(df.schema, "legs.conditions.checks"))
    assert(!SchemaUtils.isNonNestedArray(df.schema, "legs.conditions.checks.checkNums"))
    assert(!SchemaUtils.isNonNestedArray(df.schema, "id"))
    assert(!SchemaUtils.isNonNestedArray(df.schema, "legs.legid"))

    val expectedOriginalSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- amount: long (nullable = true)
        | |    |    |    |    |-- checks: array (nullable = true)
        | |    |    |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |    |    |-- checkNums: array (nullable = true)
        | |    |    |    |    |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- legid: long (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(df.schema.treeString, expectedOriginalSchema)

    // Checking that the original data is correct - data
    val expectedOriginalData = Seq(
      Row(1L, Seq(Row(Seq(Row(100L, Seq(Row(Seq("1", "2", "3b", "4", "5c", "6"))))), 100L))),
      Row(2L, Seq(Row(Seq(Row(200L, Seq(Row(Seq("8", "9", "10b", "11", "12c", "13"))))), 200L))),
      Row(3L, Seq(Row(Seq(Row(300L, Seq())), 300L))),
      Row(4L, Seq(Row(Seq(Row(400L, null)), 400L))),
      Row(5L, Seq(Row(Seq(), 500L))),
      Row(6L, Seq()),
      Row(7L, null)
    )
    val expectedOriginalDf = expectedOriginalData.toDfWithSchema(df.schema)
    assertSmallDatasetEquality(df, expectedOriginalDf) // checking just the data

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("legs", df)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("legs.conditions", explodedDf1, explodeContext1)
    val (explodedDf3, explodeContext3) = ExplodeTools.explodeArray("legs.conditions.checks", explodedDf2, explodeContext2)
    val (explodedDf4, explodeContext4) = ExplodeTools.explodeArray("legs.conditions.checks.checkNums", explodedDf3, explodeContext3)

    // Checking if explosion has been done correctly - schema
    val expectedExplodedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- legs: struct (nullable = false)
        | |    |-- conditions: struct (nullable = false)
        | |    |    |-- amount: long (nullable = true)
        | |    |    |-- checks: struct (nullable = false)
        | |    |    |    |-- checkNums: string (nullable = true)
        | |    |    |    |-- higgs: null (nullable = true)
        | |    |-- legid: long (nullable = true)
        | |-- legs_id: long (nullable = false)
        | |-- legs_size: integer (nullable = false)
        | |-- legs_idx: integer (nullable = true)
        | |-- legs_conditions_id: long (nullable = false)
        | |-- legs_conditions_size: integer (nullable = false)
        | |-- legs_conditions_idx: integer (nullable = true)
        | |-- legs_conditions_checks_id: long (nullable = false)
        | |-- legs_conditions_checks_size: integer (nullable = false)
        | |-- legs_conditions_checks_idx: integer (nullable = true)
        | |-- legs_conditions_checks_checkNums_id: long (nullable = false)
        | |-- legs_conditions_checks_checkNums_size: integer (nullable = false)
        | |-- legs_conditions_checks_checkNums_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val actualExplodedSchema = explodedDf4.schema.treeString.replaceAll("higgs_\\d+","higgs")
    assertSchema(actualExplodedSchema, expectedExplodedSchema)

    // Checking if explosion has been done correctly - data
    assert(explodedDf4.count() == 17)
    val expectedExplodedData = Seq(
      Row(1L, Row(Row(100L, Row("1", null)), 100L), 0L, 1, 0, 0L, 1, 0, 0L, 1, 0, 0L, 6, 0),
      Row(1L, Row(Row(100L, Row("2", null)), 100L), 0L, 1, 0, 0L, 1, 0, 0L, 1, 0, 0L, 6, 1),
      Row(1L, Row(Row(100L, Row("3b", null)), 100L), 0L, 1, 0, 0L, 1, 0, 0L, 1, 0, 0L, 6, 2),
      Row(1L, Row(Row(100L, Row("4", null)), 100L), 0L, 1, 0, 0L, 1, 0, 0L, 1, 0, 0L, 6, 3),
      Row(1L, Row(Row(100L, Row("5c", null)), 100L), 0L, 1, 0, 0L, 1, 0, 0L, 1, 0, 0L, 6, 4)
    )
    val expectedExplodedDf = expectedExplodedData.toDfWithSchema(explodedDf4.schema)
    assertSmallDatasetEquality(explodedDf4.limit(5), expectedExplodedDf) // checking just the data: just 5 first rows

    // Check the filter generator as well
    val explodeConditionFilter = explodeContext4.getControlFrameworkFilter
    val expectedExplodeFilter = "((((true AND (coalesce(legs_conditions_checks_checkNums_idx, 0) = 0)) AND (coalesce(legs_conditions_checks_idx, 0) = 0)) AND (coalesce(legs_conditions_idx, 0) = 0)) AND (coalesce(legs_idx, 0) = 0))"
    assert(explodeConditionFilter.toString == expectedExplodeFilter)

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf4, explodeContext4)

    // Checking if restoration has been done correctly - data + schema
    assertSmallDatasetEquality(restoredDf, expectedOriginalDf) // restored schema+data should be same as original
  }

  test ("Test exploding a nested array that is the only element of a struct") {
    val sample = """{"id":1,"leg":{"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"conditions":[]}}""" ::
      """{"id":4}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    // Checking that the original data is correct - schema
    val expectedOriginalSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = true)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(df.schema.treeString, expectedOriginalSchema)

    // Checking that the original data is correct - data
    val expectedOriginalData = Seq(
      Row(1L, Row(Seq(Row("b", "a"), Row("d", "c"), Row("f", "e")))),
      Row(2L, Row(Seq(Row("h", "g"), Row("j", "i"), Row("l", "k")))),
      Row(3L, Row(Seq())),
      Row(4L, null)
    )
    val expectedOriginalDf = expectedOriginalData.toDfWithSchema(df.schema)
    assertSmallDatasetEquality(df, expectedOriginalDf) // checking just the data

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", df)

    // Checking if explosion has been done correctly - schema
    val expectedExplodedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- conditions: struct (nullable = true)
        | |    |    |-- action: string (nullable = true)
        | |    |    |-- check: string (nullable = true)
        | |    |-- higgs: null (nullable = true)
        | |-- leg_conditions_id: long (nullable = false)
        | |-- leg_conditions_size: integer (nullable = false)
        | |-- leg_conditions_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val actualExplodedSchema = explodedDf.schema.treeString.replaceAll("higgs_\\d+","higgs")
    assertSchema(actualExplodedSchema, expectedExplodedSchema)

    // Checking if explosion has been done correctly - data
    val expectedExplodedData = Seq(
      Row(1L, Row(Row("b", "a"), null), 0L, 3, 0),
      Row(1L, Row(Row("d", "c"), null), 0L, 3, 1),
      Row(1L, Row(Row("f", "e"), null), 0L, 3, 2),
      Row(2L, Row(Row("h", "g"), null), 8589934592L, 3, 0),
      Row(2L, Row(Row("j", "i"), null), 8589934592L, 3, 1),
      Row(2L, Row(Row("l", "k"), null), 8589934592L, 3, 2),
      Row(3L, Row(null, null), 17179869184L, 0, null),
      Row(4L, Row(null, null), 25769803776L, -1, null)
    )
    val expectedExplodedDf = expectedExplodedData.toDfWithSchema(explodedDf.schema)
    assertSmallDatasetEquality(explodedDf, expectedExplodedDf) // checking just the data

    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if restoration has been done correctly - schema
    val expectedRestoredSchema = // this schema only differs from the original in `leg`'s nullability (true -> false)
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1L, Row(Seq(Row("b", "a"), Row("d", "c"), Row("f", "e")))),
      Row(2L, Row(Seq(Row("h", "g"), Row("j", "i"), Row("l", "k")))),
      Row(3L, Row(Seq())),
      Row(4L, Row(null)) // differs from original here, it held just "null", after restoration: "Row(null)" (~ struct)
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test ("Test explosion of an array field inside a struct") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", df)
    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf, explodeContext)

    // Checking if restoration has been done correctly - schema
    val expectedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        | |    |-- legid: long (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1L, Row(Seq(Row("b", "a"), Row("d", "c"), Row("f", "e")), 100L)),
      Row(2L, Row(Seq(Row("h", "g"), Row("j", "i"), Row("l", "k")), 200L)),
      Row(3L, Row(Seq(), 300L)),
      Row(4L, Row(null, 400L))
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test ("Test explosion with an error column") {
    val sample = """{"id":1,"errors":["Error 1","Error 2"],"leg":{"legid":100,"conditions":[{"check":"1","action":"b"},{"check":"2","action":"d"},{"check":"3","action":"f"}]}}""" ::
      """{"id":2,"errors":[],"leg":{"legid":200,"conditions":[{"check":"0","action":"b"}]}}""" ::
      """{"id":3,"errors":[],"leg":{"legid":300}}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)
    val (explodedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", df)

    // Manipulate error column
    val changedDf = explodedDf.select(concat($"errors", array($"leg.conditions.check")).as("errors"),
      $"id", $"leg", $"leg_conditions_id", $"leg_conditions_size", $"leg_conditions_idx")

    val restoredDf = ExplodeTools.revertAllExplosions(changedDf, explodeContext, Some("errors"))

    // Checking if restoration has been done correctly - schema
    val expectedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        | |    |-- legid: long (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(1L, Row(Seq(Row("b", "1"), Row("d", "2"), Row("f", "3")), 100L), Seq("Error 1", "Error 2", "1", "2", "3")),
      Row(2L, Row(Seq(Row("b", "0")), 200L), Seq("0")),
      Row(3L, Row(null, 300L), Seq(null))
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test ("Test empty struct inside an array") {
    val sample = """{"order":1,"a":[{"b":"H1","c":[{"d":1,"toDrop": "drop me"}]}],"myFlag":true}""" ::
      """{"order":2,"a":[{"b":"H2","c":[]}],"myFlag":true}""" ::
      """{"order":3,"a":[{"b":"H3"}],"myFlag":true}""" ::
      """{"order":4,"a":[{}],"myFlag":true}""" ::
      """{"order":5,"a":[],"myFlag":true}""" ::
      """{"order":6,"myFlag":true}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("a", df)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("a.c", explodedDf1, explodeContext1)

    // Manipulate the exploded structs
    val changedDf = explodedDf2.nestedDropColumn("a.c.toDrop")
    val restoredDf = ExplodeTools.revertAllExplosions(changedDf, explodeContext2)

    // Checking if restoration has been done correctly - schema
    val expectedSchema =
      """root
        | |-- myFlag: boolean (nullable = true)
        | |-- order: long (nullable = true)
        | |-- a: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- b: string (nullable = true)
        | |    |    |-- c: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- d: long (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(true, 1L, Seq(Row("H1",  Seq(Row(1L))))),
      Row(true, 2L, Seq(Row("H2", Seq()))),
      Row(true, 3L, Seq(Row("H3", null))),
      Row(true, 4L, Seq(Row(null, null))),
      Row(true, 5L, Seq()),
      Row(true, 6L, null)
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  test ("Test empty struct inside an array with the only array field") {
    val sample = """{"order":1,"a":[{"c":[{"d":1}]}],"myFlag":true}""" ::
      """{"order":2,"a":[{"c":[]}],"myFlag":true}""" ::
      """{"order":3,"a":[{}],"myFlag":true}""" ::
      """{"order":4,"a":[],"myFlag":true}""" ::
      """{"order":5,"myFlag":true}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val (explodedDf1, explodeContext1) = ExplodeTools.explodeArray("a", df)
    val (explodedDf2, explodeContext2) = ExplodeTools.explodeArray("a.c", explodedDf1, explodeContext1)
    val restoredDf = ExplodeTools.revertAllExplosions(explodedDf2, explodeContext2)

    // Checking if restoration has been done correctly - schema
    val expectedSchema =
      """root
        | |-- myFlag: boolean (nullable = true)
        | |-- order: long (nullable = true)
        | |-- a: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- c: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- d: long (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    assertSchema(restoredDf.schema.treeString, expectedSchema)

    // Checking if restoration has been done correctly - data
    val expectedRestoredData = Seq(
      Row(true, 1L, Seq(Row(Seq(Row(1L))))),
      Row(true, 2L, Seq(Row(Seq()))),
      Row(true, 3L, Seq(Row(null))),
      Row(true, 4L, Seq()),
      Row(true, 5L, null)
    )
    val expectedRestoredDf = expectedRestoredData.toDfWithSchema(restoredDf.schema)
    assertSmallDatasetEquality(restoredDf, expectedRestoredDf) // checking just the data
  }

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      logger.error(s"EXPECTED:\n$expectedSchema")
      logger.error(s"ACTUAL:\n$actualSchema")
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }


}
