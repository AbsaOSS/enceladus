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
import za.co.absa.enceladus.utils.explode.ExplodeTools
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ExplosionSuite extends FunSuite with SparkTestBase {

  import spark.implicits._

  test("Test explosion of a simple array") {
    // An array of 5 elements each having 10 elements
    val sampleArray = Range(1, 6).map(a => Range(a, 10 + a).toList).toList
    val df = sampleArray.toDF()

    val expectedSchema = """root
                           | |-- value_id: long (nullable = false)
                           | |-- value_size: integer (nullable = false)
                           | |-- value_idx: integer (nullable = true)
                           | |-- value: integer (nullable = true)
                           |""".stripMargin.replace("\r\n", "\n")

    val expectedResults =
      """+--------+----------+---------+-----+
        ||value_id|value_size|value_idx|value|
        |+--------+----------+---------+-----+
        ||0       |10        |0        |1    |
        ||0       |10        |1        |2    |
        ||0       |10        |2        |3    |
        ||0       |10        |3        |4    |
        ||0       |10        |4        |5    |
        ||0       |10        |5        |6    |
        ||0       |10        |6        |7    |
        ||0       |10        |7        |8    |
        ||0       |10        |8        |9    |
        ||0       |10        |9        |10   |
        ||1       |10        |0        |2    |
        ||1       |10        |1        |3    |
        ||1       |10        |2        |4    |
        ||1       |10        |3        |5    |
        ||1       |10        |4        |6    |
        ||1       |10        |5        |7    |
        ||1       |10        |6        |8    |
        ||1       |10        |7        |9    |
        ||1       |10        |8        |10   |
        ||1       |10        |9        |11   |
        |+--------+----------+---------+-----+
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
        | |-- static: integer (nullable = false)
        | |-- value_id: long (nullable = false)
        | |-- value_size: integer (nullable = false)
        | |-- value_idx: integer (nullable = true)
        | |-- value: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedExplodedResults =
      """+------+--------+----------+---------+-----+
        ||static|value_id|value_size|value_idx|value|
        |+------+--------+----------+---------+-----+
        ||1     |0       |10        |0        |1    |
        ||1     |0       |10        |1        |2    |
        ||1     |0       |10        |2        |3    |
        ||1     |0       |10        |3        |4    |
        ||1     |0       |10        |4        |5    |
        ||1     |0       |10        |5        |6    |
        ||1     |0       |10        |6        |7    |
        ||1     |0       |10        |7        |8    |
        ||1     |0       |10        |8        |9    |
        ||1     |0       |10        |9        |10   |
        ||1     |1       |10        |0        |2    |
        ||1     |1       |10        |1        |3    |
        ||1     |1       |10        |2        |4    |
        ||1     |1       |10        |3        |5    |
        ||1     |1       |10        |4        |6    |
        ||1     |1       |10        |5        |7    |
        ||1     |1       |10        |6        |8    |
        ||1     |1       |10        |7        |9    |
        ||1     |1       |10        |8        |10   |
        ||1     |1       |10        |9        |11   |
        |+------+--------+----------+---------+-----+
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
      fail("Actual conformed dataset data does not match the expected data (see above).")
    }
  }

}
