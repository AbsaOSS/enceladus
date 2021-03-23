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

import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigValueFactory}
import za.co.absa.enceladus.common.RecordIdGenerationSuite.{SomeData, SomeDataWithId}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import RecordIdGeneration._
import IdType._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecordIdGenerationSuite extends AnyFlatSpec with Matchers with SparkTestBase {
  import spark.implicits._

  val data1 = Seq(
    SomeData("abc", 12),
    SomeData("def", 34),
    SomeData("xyz", 56)
  )

  "RecordIdColumnByStrategy" should s"do noop with $NoId" in {
    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "idColumnWontBeUsed", NoId)

    df1.collectAsList() shouldBe updatedDf1.collectAsList()
  }

  it should s"always yield the same IDs with ${StableHashId}" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "stableId", StableHashId)
    val updatedDf2 = addRecordIdColumnByStrategy(df1, "stableId", StableHashId)

    updatedDf1.as[SomeDataWithId].collect() should contain theSameElementsInOrderAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach { updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.length shouldBe 3
    }
  }

  it should s"yield the different IDs with $TrueUuids" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "trueId", TrueUuids)
    val updatedDf2 = addRecordIdColumnByStrategy(df1, "trueId", TrueUuids)

    updatedDf1.as[SomeDataWithId].collect() shouldNot contain theSameElementsAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach { updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.length shouldBe 3
      updatedData.foreach(entry => UUID.fromString(entry.enceladus_record_id))
    }
  }

  "RecordIdGenerationStrategyFromConfig" should "correctly load uuidType from config (case insensitive)" in {

    def configWithStrategyValue(value: String): Config =
      ConfigFactory.empty().withValue("enceladus.recordId.generation.strategy", ConfigValueFactory.fromAnyRef(value))

    getRecordIdGenerationStrategyFromConfig(configWithStrategyValue("UUiD")) shouldBe TrueUuids
    getRecordIdGenerationStrategyFromConfig(configWithStrategyValue("StaBleHASHiD")) shouldBe StableHashId
    getRecordIdGenerationStrategyFromConfig(configWithStrategyValue("nOnE")) shouldBe NoId

    val caughtException = the[ConfigException.BadValue] thrownBy {
      getRecordIdGenerationStrategyFromConfig(configWithStrategyValue("InVaLiD"))
    }
    caughtException.getMessage should include("Invalid value at 'enceladus.recordId.generation.strategy'")
  }

}

object RecordIdGenerationSuite {

  case class SomeData(value1: String, value2: Int)

  case class SomeDataWithId(value1: String, value2: Int, enceladus_record_id: String)

}
