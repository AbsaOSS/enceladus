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
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.conformance.samples._
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import org.json4s._
import org.json4s.jackson._
import za.co.absa.enceladus.utils.fs.FileReader

class InterpreterSuite extends FunSuite with SparkTestBase with BeforeAndAfterAll with LoggerTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll
    TradeConformance.deleteTestData()
    TradeConformance.createTestDataFiles()
  }

  override def afterAll(): Unit = {
    TradeConformance.deleteTestData()
    super.afterAll
  }

  def testEndToEndDynamicConformance(useExperimentalMappingRule: Boolean): Unit = {
    // Enable Conformance Framework
    import za.co.absa.atum.AtumImplicits._
    spark.enableControlMeasuresTracking("src/test/testData/employee/2017/11/01/_INFO", "src/test/testData/_testOutput/_INFO")

    //configure conf value
    spark.sessionState.conf.setConfString("co.za.absa.enceladus.confTest", "hello :)")

    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    implicit val progArgs: ConformanceConfig = ConformanceConfig(
      experimentalMappingRule = Option(useExperimentalMappingRule),reportDate = "2017-11-01")
    val enableCF = true
    val isCatalystWorkaroundEnabled = true

    import spark.implicits._
    val mappingTablePattern = "{0}/{1}/{2}"

    val dfs = DataSource.getDataFrame(EmployeeConformance.employeeDS.hdfsPath, "2017-11-01", mappingTablePattern)

    mockWhen(dao.getDataset("Employee Conformance", 1)) thenReturn EmployeeConformance.employeeDS
    mockWhen(dao.getMappingTable("country", 0)) thenReturn EmployeeConformance.countryMT
    mockWhen(dao.getMappingTable("department", 0)) thenReturn EmployeeConformance.departmentMT
    mockWhen(dao.getMappingTable("role", 0)) thenReturn EmployeeConformance.roleMT
    mockWhen(dao.getSchema("Employee", 0)) thenReturn dfs.schema
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val conformed = DynamicInterpreter.interpret(EmployeeConformance.employeeDS, dfs)

    val data = conformed.as[ConformedEmployee].collect.sortBy(_.employee_id).toList
    val expected = EmployeeConformance.conformedEmployees.sortBy(_.employee_id).toList

    // perform the write
    conformed.coalesce(1).orderBy($"employee_id".asc).write.mode("overwrite").parquet("src/test/testData/_testOutput")

    spark.disableControlMeasuresTracking()

    val infoFile = FileReader.readFileAsString("src/test/testData/_testOutput/_INFO")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val checkpoints = parseJson(infoFile).extract[ControlMeasure].checkpoints

    assertResult(expected)(data)
    // test drop
    assert(!conformed.columns.contains("ToBeDropped"))

    // check that all the expected checkpoints are there
    assert(checkpoints.lengthCompare(9) == 0)

    checkpoints.foreach({ cp =>
      assert(cp.controls(0).controlValue === "8")
      assert(cp.controls(1).controlValue === "6")
    })
  }

  def testEndToEndArrayConformance(useExperimentalMappingRule: Boolean): Unit = {
    // Enable Conformance Framework
    import za.co.absa.atum.AtumImplicits._
    spark.enableControlMeasuresTracking("src/test/testData/_tradeData/2017/11/01/_INFO", "src/test/testData/_tradeOutput/_INFO")

    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    implicit val progArgs: ConformanceConfig = ConformanceConfig(
      experimentalMappingRule = Option(useExperimentalMappingRule),
      reportDate = "2017-11-01")
    val enableCF = true
    val isCatalystWorkaroundEnabled = true

    import spark.implicits._
    val mappingTablePattern = "{0}/{1}/{2}"

    val dfs = DataSource.getDataFrame(TradeConformance.tradeDS.hdfsPath, "2017-11-01", mappingTablePattern)

    mockWhen(dao.getDataset("Trade Conformance", 1)) thenReturn TradeConformance.tradeDS
    mockWhen(dao.getMappingTable("country", 0)) thenReturn TradeConformance.countryMT
    mockWhen(dao.getMappingTable("currency", 0)) thenReturn TradeConformance.currencyMT
    mockWhen(dao.getMappingTable("product", 0)) thenReturn TradeConformance.productMT
    mockWhen(dao.getSchema("Trade", 0)) thenReturn dfs.schema
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val conformed = DynamicInterpreter.interpret(TradeConformance.tradeDS, dfs).cache

    val data = conformed.repartition(1).orderBy($"id").toJSON.collect.mkString("\n")

    // Different results for the original mapping rule interreter algorithm and for the optmized one because of:
    // 1. Order of the columns differ
    // 2. The explode (original version) seems do not handle empty array case very well if there is an array inside an array
    val expected = if (useExperimentalMappingRule){
      TradeConformance.expectedConformedGroupExplode
    } else {
      TradeConformance.expectedConformedJson
    }

    conformed.coalesce(1).orderBy($"id").write.mode("overwrite").parquet("src/test/testData/_tradeOutput")

    spark.disableControlMeasuresTracking()

    val infoFile = FileReader.readFileAsString("src/test/testData/_tradeOutput/_INFO")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val checkpoints = parseJson(infoFile).extract[ControlMeasure].checkpoints

    if (data != expected) {
      logger.error("EXPECTED:")
      logger.error(expected)
      logger.error("ACTUAL:")
      logger.error(data)
      assert(data == expected)
    }

    // check that all the expected checkpoints are there
    assert(checkpoints.lengthCompare(12) == 0)

    checkpoints.foreach({ cp =>
      assert(cp.controls(0).controlValue === "7")
      assert(cp.controls(1).controlValue === "7")
      assert(cp.controls(2).controlValue === "28")
    })
  }

  test("End to end dynamic conformance test") {
    testEndToEndDynamicConformance(useExperimentalMappingRule = false)
  }

  test("End to end dynamic conformance test (experimental optimized mapping rule)") {
    testEndToEndDynamicConformance(useExperimentalMappingRule = true)
  }

  test("End to end array dynamic conformance test") {
    testEndToEndArrayConformance(useExperimentalMappingRule = false)
  }

  test("End to end array dynamic conformance test (experimental optimized mapping rule)") {
    testEndToEndArrayConformance(useExperimentalMappingRule = true)
  }
}
