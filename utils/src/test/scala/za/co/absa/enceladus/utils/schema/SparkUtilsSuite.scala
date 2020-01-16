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

package za.co.absa.enceladus.utils.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class SparkUtilsSuite extends FunSuite with SparkTestBase {

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  private def getDummyDataFrame: DataFrame = {
    import spark.implicits._

    Seq(1, 1, 1, 2, 1).toDF("value")
  }

  test("Test withColumnIfNotExist() when the column does not exist") {
    val expectedOutput =
      """+-----+---+
        ||value|foo|
        |+-----+---+
        ||1    |1  |
        ||1    |1  |
        ||1    |1  |
        ||2    |1  |
        ||1    |1  |
        |+-----+---+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "foo", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfOut.schema.length == 2)
    assert(dfOut.schema.head.name == "value")
    assert(dfOut.schema(1).name == "foo")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists") {
    val expectedOutput =
      """+-----+----------------------------------------------------------------------------------------------+
        ||value|errCol                                                                                        |
        |+-----+----------------------------------------------------------------------------------------------+
        ||1    |[]                                                                                            |
        ||1    |[]                                                                                            |
        ||1    |[]                                                                                            |
        ||1    |[[confLitError, E00005, Conformance Error - Special column value has changed, value, [2], []]]|
        ||1    |[]                                                                                            |
        |+-----+----------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "value", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }

  test("Test withColumnIfNotExist() when the column exists, but has a different case") {
    val expectedOutput =
      """+-----+----------------------------------------------------------------------------------------------+
        ||vAlUe|errCol                                                                                        |
        |+-----+----------------------------------------------------------------------------------------------+
        ||1    |[]                                                                                            |
        ||1    |[]                                                                                            |
        ||1    |[]                                                                                            |
        ||1    |[[confLitError, E00005, Conformance Error - Special column value has changed, vAlUe, [2], []]]|
        ||1    |[]                                                                                            |
        |+-----+----------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "vAlUe", lit(1))
    val actualOutput = dfOut.dataAsString(truncate = false)

    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")
    assert(actualOutput == expectedOutput)
  }

}
