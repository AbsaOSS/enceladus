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
import org.scalatest.FunSuite
import za.co.absa.enceladus.common.cmd.ConformanceCmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{DropConformanceRule, LiteralConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class LiteralJoinMappingRuleTest extends FunSuite with SparkTestBase with LoggerTestBase {

  def testMappingRuleWithLiteral(useExperimentalMappingRule: Boolean): Unit = {

    //For some reason this is only reproducible with files, assume that in-memory relations
    //probably have statistics allowing spark to figure out its not a cross join
    val inputDf = spark.read.option("header", "true").csv("src/test/resources/interpreter/literalJoin/data")
    val mappingDf = spark.read.option("header", "true").csv("src/test/resources/interpreter/literalJoin/mapping")

    implicit val progArgs: ConformanceCmdConfig = ConformanceCmdConfig(reportDate = "2018-03-23")
    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    val enableCF = false
    val isCatalystWorkaroundEnabled = true

    mockWhen(dao.getMappingTable("countryMT", 0)) thenReturn MappingTable(name = "countryMT", version = 0,
        hdfsPath = "countryMT", schemaName = "whatev", schemaVersion = 0, defaultMappingValue = List())

    DataSource.setData("countryMT", mappingDf)

    val conformanceDef = ConfDataset(
      name = "My dummy conformance workflow",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        LiteralConformanceRule(order = 1, outputColumn = "country", controlCheckpoint = true, value = "CZ"),
        MappingConformanceRule(order = 2, controlCheckpoint = true, mappingTable = "countryMT", mappingTableVersion = 0,
                                    attributeMappings = Map("countryCode" -> "country"),  targetAttribute = "countryName",
                                    outputColumn = "conformedCountry", isNullSafe = true),
        DropConformanceRule(order = 3,   controlCheckpoint = false, outputColumn = "country")
      )
    )

    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val confd = DynamicInterpreter.interpret(conformanceDef, inputDf).repartition(2)

    confd.write.mode("overwrite").parquet("_testOutput")
    val readAgain = spark.read.parquet("_testOutput")

    assert(readAgain.count === 2)
  }

  test("Test mapping rule with literal in join condition - success") {
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    testMappingRuleWithLiteral(useExperimentalMappingRule = false)
  }

  test("Test mapping rule with literal in join condition - experimental mapping rule - success") {
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    testMappingRuleWithLiteral(useExperimentalMappingRule = true)
  }
}
