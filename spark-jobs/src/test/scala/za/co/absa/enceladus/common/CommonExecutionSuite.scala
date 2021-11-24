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

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.common.config.PathConfig
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.validation.ValidationLevel

class CommonExecutionSuite extends AnyFlatSpec with Matchers with SparkTestBase with MockitoSugar {

  private class CommonJobExecutionTest extends CommonJobExecution {
    def testRun(implicit dao: MenasDAO, cmd: StandardizationConfig): PreparationResult = {
      prepareJob()
    }
    override protected def validatePaths(pathConfig: PathConfig): Unit = {}
    override def repartitionDataFrame(df:  DataFrame, minBlockSize: Option[Long], maxBlockSize: Option[Long])
                                               (implicit spark: SparkSession): DataFrame =
      super.repartitionDataFrame(df, minBlockSize, maxBlockSize)
  }

  Seq(
    ("failed validation", Some(Validation(Map("propX" -> List("Mandatory propX is missing")))), Seq("Dataset validation failed", "Mandatory propX is missing")),
    ("missing validation", None, Seq("Dataset validation was not retrieved correctly"))
  ).foreach { case (caseName, mockedPropertiesValidation, expectedMessageSubstrings) =>

    "CommonExecution" should s"fail on invalid properties ($caseName)" in {
      implicit val dao: MenasDAO = mock[MenasDAO]
      implicit val cmd: StandardizationConfig = StandardizationConfig(datasetName = "DatasetA")

      val dataset = Dataset("DatasetA", 1, None, "", "", "SchemaA", 1, conformance = Nil,
        properties = Some(Map("prop1" -> "value1")), propertiesValidation = mockedPropertiesValidation) // (not) validated props
      Mockito.when(dao.getDataset("DatasetA", 1, ValidationLevel.ForRun)).thenReturn(dataset)
      doNothing.when(dao).authenticate()


      val commonJob = new CommonJobExecutionTest

      val exceptionMessage = intercept[IllegalStateException](commonJob.testRun).getMessage
      expectedMessageSubstrings.foreach { subMsg =>
        exceptionMessage should include(subMsg)
      }
    }
  }

  "repartitionDataFrame" should "pass on empty data" in {
    val schema = new StructType()
      .add("not_important", StringType, nullable = true)
    val df = spark.read.schema(schema).parquet("src/test/resources/data/empty")
    println(df.rdd.getNumPartitions)
    val commonJob = new CommonJobExecutionTest
    val result = commonJob.repartitionDataFrame(df, Option(1), Option(2))
    result shouldBe df
  }

}
