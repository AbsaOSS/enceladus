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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.NumericType
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class LocalMappingTableSuite extends AnyWordSpec with SparkTestBase {

  import spark.implicits._

  "a local mapping table" should {

    // A simple mapping table
    // root
    //  |-- id: integer
    //  |-- val: string
    //  |-- arr: array
    //  |    |-- element: integer
    val dfMt = List((1, "a", Array(1, 2, 3)), (2, "b", Array(4, 5, 6)), (3, "c", Array(7, 8, 9)))
      .toDF("id", "val", "arr")

    // A mapping table with arrays and structs
    // root
    //  |-- arst: array
    //  |    |-- element: struct
    //  |    |    |-- a: string
    //  |    |    |-- b: long
    //  |    |    |-- c: boolean
    //  |-- flag: boolean
    //  |-- id: long
    //  |-- sval: struct
    //  |    |-- d: string
    //  |    |-- e: long
    //  |    |-- f: boolean
    //  |-- val: string
    val complexMt =
    """{"id":1,"arst":[{"a":"a1","b":11,"c":true}],"val":"v1","sval":{"d":"d1","e":21,"f":true},"flag":true}""" ::
      """{"id":2,"arst":[{"a":"a2","b":12,"c":false}],"val":"v2","sval":{"d":"d2","e":22,"f":false},"flag":true}""" ::
      """{"id":3,"arst":[{"a":"a3","b":13,"c":true}],"val":"v3","sval":{"d":"d3","e":23,"f":true},"flag":true}""" :: Nil
    val dfComplexMt = JsonUtils.getDataFrameFromJson(spark, complexMt)

    "be created" when {
      "a simple mapping table dataframe is provided" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), Map(""->"val"))

        assert(localMt.keyTypes.length == 1)
        assert(localMt.rowCount == 3)

        assert(localMt.outputColumns.values.toSeq == Seq("val"))
        assert(localMt.keyTypes.head.isInstanceOf[NumericType])
        assert(localMt.getRowWithDefault(Seq(1), 0) == "a")
        assert(localMt.getRowWithDefault(Seq(2), 0) == "b")
        assert(localMt.getRowWithDefault(Seq(3), 0) == "c")
        assert(localMt.getRowWithDefault(Seq(4), 0) == 0)
      }

      "a struct type target attribute is provided" in {
        val localMt = LocalMappingTable(dfComplexMt, Seq("id"), Map(""->"sval"))

        assert(localMt.keyTypes.length == 1)
        assert(localMt.rowCount == 3)

        assert(localMt.outputColumns.values.toSeq == Seq("sval"))
        assert(localMt.keyTypes.head.isInstanceOf[NumericType])
        assert(localMt.getRowWithDefault(Seq(1),0).isInstanceOf[Row])
      }

      "a join key is inside a struct" in {
        val localMt = LocalMappingTable(dfComplexMt, Seq("sval.e"), Map(""->"sval"))

        assert(localMt.keyTypes.length == 1)
        assert(localMt.rowCount == 3)

        assert(localMt.outputColumns.values.toSeq == Seq("sval"))
        assert(localMt.keyTypes.head.isInstanceOf[NumericType])
        assert(localMt.getRowWithDefault(Seq(21),0).isInstanceOf[Row])

      }

      "a join condition having 2 keys" in {
        val localMt = LocalMappingTable(dfComplexMt, Seq("id", "sval.e"), Map(""->"sval"))

        assert(localMt.keyTypes.length == 2)
        assert(localMt.rowCount == 3)

        assert(localMt.outputColumns.values.toSeq == Seq("sval"))
        assert(localMt.keyTypes.head.isInstanceOf[NumericType])
        assert(localMt.keyTypes(1).isInstanceOf[NumericType])
        assert(localMt.getRowWithDefault(Seq(1, 21), 0).isInstanceOf[Row])
      }

      "no join condition" in {
        val localMt = LocalMappingTable(dfMt, Nil, Map(""->"val"))
        assert(localMt.keyTypes.length == 0)
        assert(localMt.rowCount == 1)
        assert(localMt.outputColumns.values.toSeq == Seq("val"))
      }
    }

    "throw an exception" when {
      "a join key does not exists in the schema" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfMt, Seq("dummy"), Map(""->"val"))
        }
      }

      "a target attribute provided does not exist in the schema" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfMt, Seq("id"), Map(""->"dummy"))
        }
      }

      "a join key is an array" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfMt, Seq("arr"), Map(""->"val"))
        }
      }

      "a join key is a struct" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfComplexMt, Seq("sval"), Map(""->"val"))
        }
      }

      "a join key is inside an array" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfComplexMt, Seq("arst.a"), Map(""->"val"))
        }
      }

      "a target attribute is an array" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfMt, Seq("id"), Map(""->"arr"))
        }
      }

      "a target attribute is inside an array" in {
        intercept[IllegalArgumentException] {
          LocalMappingTable(dfComplexMt, Seq("id"), Map(""->"arst.a"))
        }
      }

    }
  }

}
