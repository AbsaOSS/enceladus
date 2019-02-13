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

package za.co.absa.enceladus.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.utils.explode.ExplodeTools
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ExplosionSuite extends FunSuite with SparkTestBase {

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

    val expectedResults =
      """+-----+--------+----------+---------+
        ||value|value_id|value_size|value_idx|
        |+-----+--------+----------+---------+
        ||1    |0       |10        |0        |
        ||2    |0       |10        |1        |
        ||3    |0       |10        |2        |
        ||4    |0       |10        |3        |
        ||5    |0       |10        |4        |
        ||6    |0       |10        |5        |
        ||7    |0       |10        |6        |
        ||8    |0       |10        |7        |
        ||9    |0       |10        |8        |
        ||10   |0       |10        |9        |
        ||2    |1       |10        |0        |
        ||3    |1       |10        |1        |
        ||4    |1       |10        |2        |
        ||5    |1       |10        |3        |
        ||6    |1       |10        |4        |
        ||7    |1       |10        |5        |
        ||8    |1       |10        |6        |
        ||9    |1       |10        |7        |
        ||10   |1       |10        |8        |
        ||11   |1       |10        |9        |
        |+-----+--------+----------+---------+
        |only showing top 20 rows
        |""".stripMargin.replace("\r\n", "\n")


    val (expldedDf, explodeContext) = ExplodeTools.explodeArray("value", df)
    val actualResults = showString(expldedDf)

    assert(explodeContext.explosions.nonEmpty)
    assertSchema(expldedDf.schema.treeString, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test a simple array reconstruction") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val df = sampleArray.toDF().withColumn("static", lit(1))

    val expectedExplodedSchema =
      """root
        | |-- value: integer (nullable = true)
        | |-- static: integer (nullable = false)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedExplodedResults =
      """+-----+------+--------+----------+---------+
        ||value|static|value_id|value_size|value_idx|
        |+-----+------+--------+----------+---------+
        ||1    |1     |0       |10        |0        |
        ||2    |1     |0       |10        |1        |
        ||3    |1     |0       |10        |2        |
        ||4    |1     |0       |10        |3        |
        ||5    |1     |0       |10        |4        |
        ||6    |1     |0       |10        |5        |
        ||7    |1     |0       |10        |6        |
        ||8    |1     |0       |10        |7        |
        ||9    |1     |0       |10        |8        |
        ||10   |1     |0       |10        |9        |
        ||2    |1     |1       |10        |0        |
        ||3    |1     |1       |10        |1        |
        ||4    |1     |1       |10        |2        |
        ||5    |1     |1       |10        |3        |
        ||6    |1     |1       |10        |4        |
        ||7    |1     |1       |10        |5        |
        ||8    |1     |1       |10        |6        |
        ||9    |1     |1       |10        |7        |
        ||10   |1     |1       |10        |8        |
        ||11   |1     |1       |10        |9        |
        |+-----+------+--------+----------+---------+
        |only showing top 20 rows
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredSchema =
      """root
        | |-- static: integer (nullable = false)
        | |-- value: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredResults =
      """+------+-----------------------------------+
        ||static|value                              |
        |+------+-----------------------------------+
        ||1     |[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]    |
        ||1     |[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]   |
        ||1     |[3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  |
        ||1     |[4, 5, 6, 7, 8, 9, 10, 11, 12, 13] |
        ||1     |[5, 6, 7, 8, 9, 10, 11, 12, 13, 14]|
        |+------+-----------------------------------+
        |""".stripMargin.replace("\r\n", "\n")


    val (expldedDf, explodeContext) = ExplodeTools.explodeArray("value", df)

    val restoredDf = ExplodeTools.revertAllExplosions(expldedDf, explodeContext)

    val actualExplodedResults = showString(expldedDf)
    val actualRestoredResults = showString(restoredDf)

    // Checking if explosion has been done correctly
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(expldedDf.schema.treeString, expectedExplodedSchema)
    assertResults(actualExplodedResults, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)
    assertResults(actualRestoredResults, expectedRestoredResults)
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

    val expectedExplodedResults =
      """+-----+------+--------+----------+---------+----------+------------+-----------+
        ||value|static|value_id|value_size|value_idx|value_id_1|value_size_1|value_idx_1|
        |+-----+------+--------+----------+---------+----------+------------+-----------+
        ||1    |1     |0       |2         |0        |0         |6           |0          |
        ||2    |1     |0       |2         |0        |0         |6           |1          |
        ||3    |1     |0       |2         |0        |0         |6           |2          |
        ||4    |1     |0       |2         |0        |0         |6           |3          |
        ||5    |1     |0       |2         |0        |0         |6           |4          |
        ||6    |1     |0       |2         |0        |0         |6           |5          |
        ||7    |1     |0       |2         |1        |1         |7           |0          |
        ||8    |1     |0       |2         |1        |1         |7           |1          |
        ||9    |1     |0       |2         |1        |1         |7           |2          |
        ||10   |1     |0       |2         |1        |1         |7           |3          |
        |+-----+------+--------+----------+---------+----------+------------+-----------+
        |only showing top 10 rows
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredSchema =
      """root
        | |-- static: integer (nullable = false)
        | |-- value: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: integer (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredResults =
      """+------+---------------------------------------------------------------------+
        ||static|value                                                                |
        |+------+---------------------------------------------------------------------+
        ||1     |[[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12, 13]]                      |
        ||1     |[[201, 202, 203, 204, 205, 206], [207, 208, 209, 210, 211, 212, 213]]|
        ||1     |[[301, 302, 303, 304, 305, 306], [307, 308, 309, 310, 311, 312, 313]]|
        ||1     |[[401, 402, 403, 404, 405, 406], [407, 408, 409, 410, 411, 412, 413]]|
        |+------+---------------------------------------------------------------------+
        |""".stripMargin.replace("\r\n", "\n")

    val (expldedDf1, explodeContext1) = ExplodeTools.explodeArray("value", df)
    val (expldedDf2, explodeContext2) = ExplodeTools.explodeArray("value", expldedDf1, explodeContext1)

    val restoredDf = ExplodeTools.revertAllExplosions(expldedDf2, explodeContext2)

    val actualExplodedResults = showString(expldedDf2, 10)
    val actualRestoredResults = showString(restoredDf)

    // Checking if explosion has been done correctly
    assert(explodeContext2.explosions.size == 2)
    assertSchema(expldedDf2.schema.treeString, expectedExplodedSchema)
    assertResults(actualExplodedResults, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)
    assertResults(actualRestoredResults, expectedRestoredResults)
  }

  test("Test handling of empty and null arrays") {
    val sample = Seq("""{"value":[1,2,3,4,5,6,7,8,9,10],"static":1}""",
      """{"value":[2,3,4,5,6,7,8,9,10,11],"static":2}""",
      """{"value":[],"static":3}""",
      """{"static":4}""")
    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val expectedExplodedSchema =
      """root
        | |-- static: long (nullable = true)
        | |-- value: long (nullable = true)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedExplodedResults =
      """+------+----------+---------+-----+
        ||static|value_size|value_idx|value|
        |+------+----------+---------+-----+
        ||4     |-1        |null     |null |
        ||3     |0         |null     |null |
        ||1     |10        |0        |1    |
        ||2     |10        |0        |2    |
        ||1     |10        |1        |2    |
        |+------+----------+---------+-----+
        |only showing top 5 rows
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredSchema =
      """root
        | |-- static: long (nullable = true)
        | |-- value: array (nullable = true)
        | |    |-- element: long (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredResults =
      """+------+--------------------------------+
        ||static|value                           |
        |+------+--------------------------------+
        ||1     |[1, 2, 3, 4, 5, 6, 7, 8, 9, 10] |
        ||2     |[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]|
        ||3     |[]                              |
        ||4     |null                            |
        |+------+--------------------------------+
        |""".stripMargin.replace("\r\n", "\n")


    val (expldedDf, explodeContext) = ExplodeTools.explodeArray("value", df)

    val restoredDf = ExplodeTools.revertAllExplosions(expldedDf, explodeContext)

    val actualExplodedResults = showString(expldedDf
      .select($"static", $"value_size", $"value_idx", $"value")
      .orderBy($"value_size", $"value_idx", $"static"), 5)
    val actualRestoredResults = showString(restoredDf)

    // Checking if explosion has been done correctly
    assert(explodeContext.explosions.nonEmpty)
    assertSchema(expldedDf.schema.treeString, expectedExplodedSchema)
    assertResults(actualExplodedResults, expectedExplodedResults)

    // Checking if restoration has been done correctly
    assertSchema(restoredDf.schema.treeString, expectedRestoredSchema)
    assertResults(actualRestoredResults, expectedRestoredResults)
  }

  test("Test deconstruct()") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val expectedDeconstructedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- legid: long (nullable = true)
        | |-- proton: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- action: string (nullable = true)
        | |    |    |-- check: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedDeconstructedData =
      """+---+-----+------------------------+
        ||id |leg  |proton                  |
        |+---+-----+------------------------+
        ||1  |[100]|[[b, a], [d, c], [f, e]]|
        ||2  |[200]|[[h, g], [j, i], [l, k]]|
        ||3  |[300]|[]                      |
        ||4  |[400]|null                    |
        |+---+-----+------------------------+
        |""".stripMargin.replace("\r\n", "\n")


    val (df2, fld) = ExplodeTools.deconstruct(df, "leg.conditions")

    val actualResults = showString(df2, 5)

    assertSchema(df2.schema.treeString, expectedDeconstructedSchema)
    assertResults(actualResults, actualResults)
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

    println("Original")
    df.toJSON.collect().foreach(println)
    df.printSchema()
    df.show(false)

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

    val expectedOriginalResults =
      """+---+----------------------------------------------+
        ||id |legs                                          |
        |+---+----------------------------------------------+
        ||1  |[[[[100, [[[1, 2, 3b, 4, 5c, 6]]]]], 100]]    |
        ||2  |[[[[200, [[[8, 9, 10b, 11, 12c, 13]]]]], 200]]|
        ||3  |[[[[300, []]], 300]]                          |
        ||4  |[[[[400,]], 400]]                             |
        ||5  |[[[], 500]]                                   |
        ||6  |[]                                            |
        ||7  |null                                          |
        |+---+----------------------------------------------+
        |""".stripMargin.replace("\r\n", "\n")

    val expectedExplodedSchema =
      """root
        | |-- static: long (nullable = true)
        | |-- value: long (nullable = true)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedExplodedResults =
      """+------+----------+---------+-----+
        ||static|value_size|value_idx|value|
        |+------+----------+---------+-----+
        ||4     |-1        |null     |null |
        ||3     |0         |null     |null |
        ||1     |10        |0        |1    |
        ||2     |10        |0        |2    |
        ||1     |10        |1        |2    |
        |+------+----------+---------+-----+
        |only showing top 5 rows
        |""".stripMargin.replace("\r\n", "\n")

    val expectedRestoredSchema =
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

    val expectedRestoredResults =
      """+------+--------------------------------+
        ||static|value                           |
        |+------+--------------------------------+
        ||1     |[1, 2, 3, 4, 5, 6, 7, 8, 9, 10] |
        ||2     |[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]|
        ||3     |[]                              |
        ||4     |null                            |
        |+------+--------------------------------+
        |""".stripMargin.replace("\r\n", "\n")



    val (expldedDf1, explodeContext1) = ExplodeTools.explodeArray("legs", df)
    val (expldedDf2, explodeContext2) = ExplodeTools.explodeArray("legs.conditions", expldedDf1, explodeContext1)
    //val (expldedDf3, explodeContext3) = ExplodeTools.explodeArray("legs.conditions.checks", expldedDf2, explodeContext2)
    //val (expldedDf4, explodeContext4) = ExplodeTools.explodeArray("legs.conditions.checks.checkNums", expldedDf3, explodeContext3)

//    val actualExplodedResults = showString(expldedDf
//      .select($"static", $"value_size", $"value_idx", $"value")
//      .orderBy($"value_size", $"value_idx", $"static"), 5)
//    val actualRestoredResults = showString(restoredDf)


    println("Exploded 1")
    expldedDf1.toJSON.collect().foreach(println)
    expldedDf1.printSchema()
    expldedDf1.show(false)

    println("Exploded 2")
    expldedDf2.toJSON.collect().foreach(println)

    expldedDf2.printSchema()

    expldedDf2.show(false)

    val restoredDf = ExplodeTools.revertAllExplosions(expldedDf2, explodeContext2)

    restoredDf.printSchema()

    restoredDf.show(false)

    restoredDf.toJSON.collect().foreach(println)
  }

  test ("Test explosion of an array field inside a struct") {
    val sample = """{"id":1,"leg":{"legid":100,"conditions":[{"check":"a","action":"b"},{"check":"c","action":"d"},{"check":"e","action":"f"}]}}""" ::
      """{"id":2,"leg":{"legid":200,"conditions":[{"check":"g","action":"h"},{"check":"i","action":"j"},{"check":"k","action":"l"}]}}""" ::
      """{"id":3,"leg":{"legid":300,"conditions":[]}}""" ::
      """{"id":4,"leg":{"legid":400}}""" :: Nil

    val df = JsonUtils.getDataFrameFromJson(spark, sample)

    val (expldedDf, explodeContext) = ExplodeTools.explodeArray("leg.conditions", df)
    val restoredDf = ExplodeTools.revertAllExplosions(expldedDf, explodeContext)

    val expectedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- leg: struct (nullable = false)
        | |    |-- legid: long (nullable = true)
        | |    |-- conditions: array (nullable = true)
        | |    |    |-- element: struct (containsNull = true)
        | |    |    |    |-- action: string (nullable = true)
        | |    |    |    |-- check: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedData =
      """+---+-------------------------------+
        ||id |leg                            |
        |+---+-------------------------------+
        ||1  |[[[b, a], [d, c], [f, e]], 100]|
        ||2  |[[[h, g], [j, i], [l, k]], 200]|
        ||3  |[[], 300]                      |
        ||4  |[, 400]                        |
        |+---+-------------------------------+
        |""".stripMargin.replace("\r\n", "\n")

    val actualResults = showString(restoredDf, 5)

    assertSchema(restoredDf.schema.treeString, expectedSchema)
    assertResults(actualResults, actualResults)
  }

  // Call showString() by reflection since it is private
  // Thanks https://stackoverflow.com/a/51218800/1038282
  private def showString(df: DataFrame, numRows: Int = 20): String = {
    val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString",
      classOf[Int], classOf[Int], classOf[Boolean])
    showString.setAccessible(true)
    showString.invoke(df, numRows.asInstanceOf[Object], 0.asInstanceOf[Object],
      false.asInstanceOf[Object]).asInstanceOf[String]
  }

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      logger.error(s"EXPECTED:\n$expectedSchema")
      logger.error(s"ACTUAL:\n$actualSchema")
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  private def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (actualResults != expectedResults) {
      logger.error(s"EXPECTED:\n$expectedResults")
      logger.error(s"ACTUAL:\n$actualResults")
      fail("Actual conformed dataset data does not match the expected data (see above).")
    }
  }

}
