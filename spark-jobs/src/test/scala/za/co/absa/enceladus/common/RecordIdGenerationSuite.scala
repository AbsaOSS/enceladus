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

package za.co.absa.enceladus.common

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.common.RecordIdGeneration.UuidType
import za.co.absa.enceladus.common.RecordIdGenerationSuite.{SomeData, SomeDataWithId}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.udf.UDFLibrary

class RecordIdGenerationSuite extends FlatSpec with Matchers with SparkTestBase {
  import spark.implicits._

  val data1 = Seq(
    SomeData("abc", 12),
    SomeData("def", 34),
    SomeData("xyz", 56)
  )

  "RecordIdColumnByStrategy" should s"do noop with ${UuidType.NoUuids}" in {
    implicit val udfLib = UDFLibrary()

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = RecordIdGeneration.addRecordIdColumnByStrategy(df1, UuidType.NoUuids)

    df1.collectAsList() shouldBe updatedDf1.collectAsList()
  }

  it should s"always yield the same IDs with ${UuidType.PseudoUuids}" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = RecordIdGeneration.addRecordIdColumnByStrategy(df1, UuidType.PseudoUuids)(UDFLibrary())
    val updatedDf2 = RecordIdGeneration.addRecordIdColumnByStrategy(df1, UuidType.PseudoUuids)(UDFLibrary())

    updatedDf1.as[SomeDataWithId].collect() should contain theSameElementsInOrderAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach{ updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.size shouldBe 3
      updatedData.foreach(entry => UUID.fromString(entry.enceladus_record_id))
    }
  }

  it should s"yield the different IDs with ${UuidType.TrueUuids}" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = RecordIdGeneration.addRecordIdColumnByStrategy(df1, UuidType.TrueUuids)(UDFLibrary())
    val updatedDf2 = RecordIdGeneration.addRecordIdColumnByStrategy(df1, UuidType.TrueUuids)(UDFLibrary())

    updatedDf1.as[SomeDataWithId].collect() shouldNot contain theSameElementsAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach{ updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.size shouldBe 3
      updatedData.foreach(entry => UUID.fromString(entry.enceladus_record_id))
    }
  }

}

object RecordIdGenerationSuite {

  case class SomeData(value1: String, value2: Int)
  case class SomeDataWithId(value1: String, value2: Int, enceladus_record_id: String)

}
