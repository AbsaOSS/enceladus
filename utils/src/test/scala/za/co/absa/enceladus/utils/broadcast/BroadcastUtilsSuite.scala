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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.utils.error.Mapping
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, TZNormalizedSparkTestBase}

import scala.collection.mutable

class BroadcastUtilsSuite extends AnyWordSpec with TZNormalizedSparkTestBase with LoggerTestBase {

  import spark.implicits._

  // A simple mapping table
  // root
  //  |-- id: integer
  //  |-- val: string
  private val dfMt = List((1, "a", "A", true), (2, "b", "B", false), (3, "c", "C", true)).toDF("id", "val", "val1", "val2")


  // A simple dataframe
  // root
  //  |-- key1: integer
  //  |-- key2: integer
  //  |-- key3: integer
  private val df = List((1, 2, 4), (2, 3, 4), (3, 4, 4)).toDF("key1", "key2", "key3")

  private val defaultValExpr = "\"z\""

  // Expected dataframe when 'out' field contains the results of a join for 'key1'
  private val expectedResultsMatchFoundForSingleOutput =
    """{"key1":1,"key2":2,"key3":4,"out":"a"}
      |{"key1":2,"key2":3,"key3":4,"out":"b"}
      |{"key1":3,"key2":4,"key3":4,"out":"c"}"""
      .stripMargin.replace("\r\n", "\n")

  private val expectedResultsMatchFoundForMultipleOutputs =
    """{"key1":1,"key2":2,"key3":4,"out":{"valC":"a","val1C":"A","val2C":true}}
      |{"key1":2,"key2":3,"key3":4,"out":{"valC":"b","val1C":"B","val2C":false}}
      |{"key1":3,"key2":4,"key3":4,"out":{"valC":"c","val1C":"C","val2C":true}}"""
      .stripMargin.replace("\r\n", "\n")

  private val expectedResultsMatchFoundWithDefaultsForMultipleOutputs =
    """{"key1":1,"key2":2,"key3":4,"out":{"valC":"z"}}
      |{"key1":2,"key2":3,"key3":4,"out":{"valC":"z"}}
      |{"key1":3,"key2":4,"key3":4,"out":{"valC":"z"}}"""
      .stripMargin.replace("\r\n", "\n")

  // Expected dataframe when 'out' field contains the results of a failed join
  private val expectedResultsMatchNotFound =
    """{"key1":1,"key2":2,"key3":4}
      |{"key1":2,"key2":3,"key3":4}
      |{"key1":3,"key2":4,"key3":4}"""
      .stripMargin.replace("\r\n", "\n")

  // Expected dataframe when 'out' field contains the results of a failed join and a default value is specified
  private val expectedResultsMatchNotFoundDefault =
    """{"key1":1,"key2":2,"key3":4,"out":"z"}
      |{"key1":2,"key2":3,"key3":4,"out":"z"}
      |{"key1":3,"key2":4,"key3":4,"out":"z"}"""
      .stripMargin.replace("\r\n", "\n")

  private val expectedResultMatchNotFoundDefaultForMultipleOutputs =
    """{"key1":1,"key2":2,"key3":4,"out":{"valC":"z"}}
      |{"key1":2,"key2":3,"key3":4,"out":{"valC":"z"}}
      |{"key1":3,"key2":4,"key3":4,"out":{"valC":"z"}}"""
      .stripMargin.replace("\r\n", "\n")

  "registerMappingUdf()" should {

    "return a UDF that can be used for joining a dataframe with a simple mapping table" when {

      "1 UDF parameter is used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), Map("" -> "val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf1 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf1($"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf1($"key3")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "1 UDF parameter is used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), Map("" -> "val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf1 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"-> defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf1($"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf1($"key3")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "1 UDF parameter is used without a default value and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf1 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf1($"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf1($"key3")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "1 UDF parameter is used with default values and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), Map("valC"->"val", "val1C"->"val1", "val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf1 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map("val"-> defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf1($"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf1($"key3")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchFoundWithDefaultsForMultipleOutputs)
      }

      "2 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf2 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf2($"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf2($"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "2 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf2 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf2($"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf2($"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "2 UDF parameters are used without a default value and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf2 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf2($"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf2($"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "3 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf3 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "3 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf3 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "3 UDF parameters are used without a default value and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf3 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf3($"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "4 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf4 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "4 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf4 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "4 UDF parameters are used without a default value and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf4 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf4($"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "5 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf5 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "5 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf5 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "5 UDF parameters are used without defaults and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf5 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1",$"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf5($"key1", $"key1", $"key1",$"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "6 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf6 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1", $"key1", $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "6 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf6 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1", $"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1", $"key1", $"key1" ,$"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "6 UDF parameters are used without defaults and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf6 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1",$"key1", $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf6($"key1", $"key1", $"key1",$"key1", $"key2", $"key1")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "7 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf7 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1", $"key1", $"key1", $"key1",$"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1", $"key1", $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "7 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf7 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1", $"key1", $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1", $"key1", $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "7 UDF parameters are used without defaults and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf7 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1",$"key1", $"key1",
          $"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1",$"key1", $"key1",
          $"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "8 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf8 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf8($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf8($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "8 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf8 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf8($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf8($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "8 UDF parameters are used without defaults and multiple outputs" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id"), Map("valC"->"val","val1C"->"val1","val2C"->"val2"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf7 = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1",$"key1", $"key1",
          $"key1",$"key1", $"key1")).orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf7($"key1", $"key1", $"key1",$"key1", $"key1",
          $"key1",$"key1", $"key2")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForMultipleOutputs)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "9 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf9 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "9 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf9 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1",  $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }

      "10 UDF parameters are used without a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf10 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())

        val dfOut1 = df.withColumn("out", mappingUdf10($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf10($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "10 UDF parameters are used with a default value" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        val mappingUdf9 = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map("val"->defaultValExpr))

        val dfOut1 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1", $"key1"))
          .orderBy("key1")
        val dfOut2 = df.withColumn("out", mappingUdf9($"key1", $"key1", $"key1", $"key1", $"key1",
          $"key1", $"key1", $"key1", $"key1", $"key2"))
          .orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFoundForSingleOutput)
        assertResults(dfOut2, expectedResultsMatchNotFoundDefault)
      }
    }

    "throw an exception" when {

      "a join with more than 10 fields attempted" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id",
          "id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        intercept[IllegalArgumentException] {
          BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, Map())
        }
      }
    }
  }

  "registerErrorUdf()" should {

    "return a UDF that returns an error column in case of a join error" when {
      "1 UDF parameter is used" in {
        val expectedWithErrorColumn1 =
          """{"key1":1,"key2":2,"key3":4}
            |{"key1":2,"key2":3,"key3":4}
            |{"key1":3,"key2":4,"key3":4,"errCol":{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"val","rawValues":["4"],"mappings":[{"mappingTableColumn":"id","mappedDatasetColumn":"key2"}]}}"""
            .stripMargin.replace("\r\n", "\n")

        val localMt = LocalMappingTable(dfMt, Seq("id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)
        val mappings = Seq(Mapping("id", "key2"))

        val errorUdf1 = BroadcastUtils.getErrorUdf(broadcastedMt, Seq("val"), mappings)

        val dfOut = df.withColumn("errCol", errorUdf1($"key2")).orderBy("key2")

        assertResults(dfOut, expectedWithErrorColumn1)
      }

      "2 UDF parameter is used" in {
        val expectedWithErrorColumn1 =
          """{"key1":1,"key2":2,"key3":4}
            |{"key1":2,"key2":3,"key3":4}
            |{"key1":3,"key2":4,"key3":4,"errCol":{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"val","rawValues":["4","4"],"mappings":[{"mappingTableColumn":"id","mappedDatasetColumn":"key2"},{"mappingTableColumn":"id","mappedDatasetColumn":"key2"}]}}"""
            .stripMargin.replace("\r\n", "\n")

        val localMt = LocalMappingTable(dfMt, Seq("id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)
        val mappings = Seq(Mapping("id", "key2"), Mapping("id", "key2"))

        val errorUdf2 = BroadcastUtils.getErrorUdf(broadcastedMt, Seq("val"), mappings)

        val dfOut = df.withColumn("errCol", errorUdf2($"key2", $"key2")).orderBy("key2")

        assertResults(dfOut, expectedWithErrorColumn1)
      }

      "3 UDF parameter is used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)
        val mappings = Seq(Mapping("id", "key2"), Mapping("id", "key2"), Mapping("id", "key2"))

        val errorUdf3 = BroadcastUtils.getErrorUdf(broadcastedMt, Seq("val"), mappings)

        val dfOut = df.withColumn("errCol", errorUdf3($"key2", $"key2", $"key2")).orderBy("key2")
        val error = dfOut.filter(col("errCol").isNotNull).select("errCol").as[ErrorColumn].collect()(0)

        assert(dfOut.filter(col("errCol").isNull).count == 2)
        assert(error.errCol.mappings.size == 3)
        assert(error.errCol.rawValues.size == 3)
      }

      "4 UDF parameter is used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)
        val mappings = Seq(Mapping("id", "key2"), Mapping("id", "key2"), Mapping("id", "key2"), Mapping("id", "key2"))

        val errorUdf4 = BroadcastUtils.getErrorUdf(broadcastedMt, Seq("val"), mappings)

        val dfOut = df.withColumn("errCol", errorUdf4($"key2", $"key2", $"key2", $"key2")).orderBy("key2")
        val error = dfOut.filter(col("errCol").isNotNull).select("errCol").as[ErrorColumn].collect()(0)

        assert(dfOut.filter(col("errCol").isNull).count == 2)
        assert(error.errCol.mappings.size == 4)
        assert(error.errCol.rawValues.size == 4)
      }

      "5 UDF parameter is used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id"), Map(""->"val"))
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)
        val mappings = Seq(Mapping("id", "key2"), Mapping("id", "key2"), Mapping("id", "key2"), Mapping("id", "key2"),
          Mapping("id", "key2"))

        val errorUdf5 = BroadcastUtils.getErrorUdf(broadcastedMt, Seq("val"), mappings)

        val dfOut = df.withColumn("errCol", errorUdf5($"key2", $"key2", $"key2", $"key2", $"key2")).orderBy("key2")
        val error = dfOut.filter(col("errCol").isNotNull).select("errCol").as[ErrorColumn].collect()(0)

        assert(dfOut.filter(col("errCol").isNull).count == 2)
        assert(error.errCol.mappings.size == 5)
        assert(error.errCol.rawValues.size == 5)
      }
    }

    "getValueOfSparkExpression()" should {
      "return default values of proper types" when {
        "the expression returns a primitive" in {
          val v = BroadcastUtils.getValueOfSparkExpression("\"str\"")

          assert(v.isInstanceOf[String])
          assert(v.asInstanceOf[String] == "str")
        }

        "the expression returns a struct" in {
          val v = BroadcastUtils.getValueOfSparkExpression("struct(\"str1\" as a, \"str2\" as b) as s")

          assert(v.isInstanceOf[Row])
          assert(v.asInstanceOf[Row](0).toString == "str1")
          assert(v.asInstanceOf[Row](1).toString == "str2")
        }

        "the expression returns an array" in {
          val v = BroadcastUtils.getValueOfSparkExpression("array(struct(\"str1\" as a, \"str2\" as b)) as s")

          assert(v.isInstanceOf[mutable.WrappedArray[_]])
        }

      }

      "throw an exception" when {
        "an invalid expression is specified" in {
          intercept[Exception] {
            BroadcastUtils.getValueOfSparkExpression(";invalid+expression))")
          }
        }
      }
    }
  }


  private def assertResults(actualDf: DataFrame, expectedJson: String): Unit = {
    val actualJson = actualDf.toJSON.collect.mkString("\n")
    if (actualJson != expectedJson) {
      logger.error("EXPECTED:")
      logger.error(expectedJson)
      logger.error("ACTUAL:")
      logger.error(actualJson)
      fail("Actual dataframe does not match the expected one (see above).")
    }
  }

}
