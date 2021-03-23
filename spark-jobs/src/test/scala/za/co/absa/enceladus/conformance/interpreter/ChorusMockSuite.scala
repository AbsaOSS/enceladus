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

package za.co.absa.enceladus.conformance.interpreter

import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, LoggerTestBase, SparkTestBase}

case class MyMappingTable(id: Int, mappedAttr: MyMappingTableInner)
case class MyMappingTableInner(description: String, name: String)
case class MyData(id: Int, toJoin: Int)
case class MyDataConfd(id: Int, toJoin: Int, confMapping: MyMappingTableInner)

class ChorusMockSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase with HadoopFsTestBase {

  def testChorusMockData(useExperimentalMappingRule: Boolean): Unit = {
    val d = Seq(
      MyData(0, 0),
      MyData(1, 1), MyData(2, 2))

    val mapping = Seq(
      MyMappingTable(0, MyMappingTableInner(null, "whatev")),
      MyMappingTable(1, MyMappingTableInner("something", "somethingelse")))

    val inputDf = spark.createDataFrame(d)
    val mappingDf = spark.createDataFrame(mapping)

    implicit val progArgs: ConformanceConfig = ConformanceConfig(reportDate = "2018-03-23") // here we may need to specify some parameters (for certain rules)
    implicit val dao: MenasDAO = mock(classOf[MenasDAO]) // you may have to hard-code your own implementation here (if not working with menas)
    val enableCF = false
    val isCatalystWorkaroundEnabled = true

    mockWhen(dao.getMappingTable("myMappingTable", 0)) thenReturn MappingTable(name = "myMappingTable", version = 0, hdfsPath = "myMappingTable", schemaName = "whatev", schemaVersion = 0, defaultMappingValue = List())

    DataSource.setData("myMappingTable/reportDate=2018-03-23", mappingDf)

    val conformanceDef = ConfDataset(
      name = "My dummy conformance workflow", // whatever here
      version = 0, // whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        MappingConformanceRule(order = 1, controlCheckpoint = false, mappingTable = "myMappingTable", mappingTableVersion = 0,
          attributeMappings = Map("id" -> "toJoin"), targetAttribute = "mappedAttr", outputColumn = "confMapping")))

    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val confd = DynamicInterpreter().interpret(conformanceDef, inputDf).repartition(2)

    logDataFrameContent(confd)

    confd.write.mode("overwrite").parquet("_testOutput")
    val readAgain = spark.read.parquet("_testOutput")

    assert(readAgain.count === 3)
  }

  test("Test conformance of Chorus mock data") {
    testChorusMockData(useExperimentalMappingRule = false)
  }

  test("Test conformance of Chorus mock data (experimental optimized mapping rule)") {
    testChorusMockData(useExperimentalMappingRule = true)
  }
}
