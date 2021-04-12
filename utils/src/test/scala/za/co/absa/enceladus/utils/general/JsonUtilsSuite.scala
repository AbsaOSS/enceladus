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

package za.co.absa.enceladus.utils.general

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class JsonUtilsSuite extends AnyFunSuite with SparkTestBase {
  test("Test JSON pretty formatting from a JSON string") {
    val inputJson = """[{"id":1,"items":[{"itemid":100,"subitems":[{"elems":[{"numbers":["1","2","3b","4","5c","6"]}],"code":100}]}]}]"""
    val expected = """[ {
                     |  "id" : 1,
                     |  "items" : [ {
                     |    "itemid" : 100,
                     |    "subitems" : [ {
                     |      "elems" : [ {
                     |        "numbers" : [ "1", "2", "3b", "4", "5c", "6" ]
                     |      } ],
                     |      "code" : 100
                     |    } ]
                     |  } ]
                     |} ]""".stripMargin.replace("\r\n", "\n")

    val actual = JsonUtils.prettyJSON(inputJson)

    assert(actual == expected)
  }

  test("Test JSON pretty formatting from a Spark JSON string") {
    val inputJsons = Seq("""{"value": 1}""", """{"value": 2}""")
    val expected = "[ {\n  \"value\" : 1\n}, {\n  \"value\" : 2\n} ]"

    val actual = JsonUtils.prettySparkJSON(inputJsons)

    assert(actual == expected)
  }

  test("Test a dataframe created from a JSON") {
    val inputJson = Seq("""{"value":1}""", """{"value":2}""")

    val df = JsonUtils.getDataFrameFromJson(spark, inputJson)

    val expectedSchema = """root
                           | |-- value: long (nullable = true)
                           |""".stripMargin.replace("\r\n", "\n")
    val actualSchema = df.schema.treeString

    assert(expectedSchema == actualSchema)
  }
}
