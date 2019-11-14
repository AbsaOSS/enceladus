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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.WordSpec
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class BroadcastUtilsSuite extends WordSpec with SparkTestBase with LoggerTestBase {

  import spark.implicits._

  // A simple mapping table
  // root
  //  |-- id: integer
  //  |-- val: string
  private val dfMt = List((1, "a"), (2, "b"), (3, "c")).toDF("id", "val")


  // A simple dataframe
  // root
  //  |-- key1: integer
  //  |-- key2: integer
  private val df = List((1, 2), (2, 3), (3, 1)).toDF("key1", "key2")

  // Expected dataframe when 'out' field contains the results of a join for 'key1'
  private val expectedResultsMatchFound =
    """{"key1":1,"key2":2,"out":"a"}
      |{"key1":2,"key2":3,"out":"b"}
      |{"key1":3,"key2":1,"out":"c"}"""
      .stripMargin.replace("\r\n", "\n")

  // Expected dataframe when 'out' field contains the results of a failed join
  private val expectedResultsMatchNotFound =
    """{"key1":1,"key2":2}
      |{"key1":2,"key2":3}
      |{"key1":3,"key2":1}"""
      .stripMargin.replace("\r\n", "\n")

  "registerMappingUdf()" should {

    "return a UDF that can be used for joining a dataframe with a simple mapping table" when {

      "1 UDF parameter is used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        BroadcastUtils.registerMappingUdf("mappingUdf1", broadcastedMt)

        val dfOut = df.withColumn("out", expr(s"mappingUdf1(key1)")).orderBy("key1")

        assertResults(dfOut, expectedResultsMatchFound)
      }

      "2 UDF parameters are used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        BroadcastUtils.registerMappingUdf("mappingUdf2", broadcastedMt)

        val dfOut1 = df.withColumn("out", expr(s"mappingUdf2(key1, key1)")).orderBy("key1")
        val dfOut2 = df.withColumn("out", expr(s"mappingUdf2(key1, key2)")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFound)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "3 UDF parameters are used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        BroadcastUtils.registerMappingUdf("mappingUdf3", broadcastedMt)

        val dfOut1 = df.withColumn("out", expr(s"mappingUdf3(key1, key1, key1)")).orderBy("key1")
        val dfOut2 = df.withColumn("out", expr(s"mappingUdf3(key1, key1, key2)")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFound)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "4 UDF parameters are used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        BroadcastUtils.registerMappingUdf("mappingUdf4", broadcastedMt)

        val dfOut1 = df.withColumn("out", expr(s"mappingUdf4(key1, key1, key1, key1)")).orderBy("key1")
        val dfOut2 = df.withColumn("out", expr(s"mappingUdf4(key1, key1, key1, key2)")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFound)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }

      "5 UDF parameters are used" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        BroadcastUtils.registerMappingUdf("mappingUdf4", broadcastedMt)

        val dfOut1 = df.withColumn("out", expr(s"mappingUdf4(key1, key1, key1, key1, key1)")).orderBy("key1")
        val dfOut2 = df.withColumn("out", expr(s"mappingUdf4(key1, key1, key1, key1, key2)")).orderBy("key1")

        assertResults(dfOut1, expectedResultsMatchFound)
        assertResults(dfOut2, expectedResultsMatchNotFound)
      }
    }

    "throw an exception" when {

      "a join without key fields is attempted" in {
        intercept[IllegalArgumentException] {
          val localMt = LocalMappingTable(dfMt, Nil, "val")
        }
      }

      "a join with more than 5 fields attempted" in {
        val localMt = LocalMappingTable(dfMt, Seq("id", "id", "id", "id", "id", "id"), "val")
        val broadcastedMt = BroadcastUtils.broadcastMappingTable(localMt)

        intercept[IllegalArgumentException] {
          BroadcastUtils.registerMappingUdf("mappingUdf6", broadcastedMt)
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
