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

import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.common.config.PathConfig
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class CommonExecutionSuite extends FlatSpec with Matchers with SparkTestBase with MockitoSugar {

  private class CommonJobExecutionTest extends CommonJobExecution {
    def testRun(implicit dao: MenasDAO, cmd: StandardizationConfig, fsUtils: FileSystemVersionUtils): PreparationResult = {
      prepareJob()
    }
    override protected def validatePaths(fsUtils: FileSystemVersionUtils, pathConfig: PathConfig): Unit = {}
  }

  Seq(
    ("failed validation", Some(Validation(Map("propX" -> List("Mandatory propX is missing")))), Seq("Dataset validation failed", "Mandatory propX is missing")),
    ("missing validation", None, Seq("Dataset validation was not retrieved correctly"))
  ).foreach { case (caseName, mockedPropertiesValidation, expectedMessageSubstrings) =>

    "CommonExecution" should s"fail on invalid properties ($caseName)" in {
      implicit val dao: MenasDAO = mock[MenasDAO]
      implicit val cmd: StandardizationConfig = StandardizationConfig(datasetName = "DatasetA")
      implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

      val dataset = Dataset("DatasetA", 1, None, "", "", "SchemaA", 1, conformance = Nil,
        properties = Some(Map("prop1" -> "value1")), propertiesValidation = mockedPropertiesValidation) // (not) validated props
      Mockito.when(dao.getDataset("DatasetA", 1, validateProperties = true)).thenReturn(dataset)

      val commonJob = new CommonJobExecutionTest

      val exceptionMessage = intercept[IllegalStateException](commonJob.testRun).getMessage
      expectedMessageSubstrings.foreach { subMsg =>
        exceptionMessage should include(subMsg)
      }
    }
  }

}
