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

package za.co.absa.enceladus.examples.interpreter.rules.custom

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.scalatest.MockitoSugar
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.fs.HadoopFsUtils
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, SparkTestBase}


case class TestInputRow(id: Int, mandatoryString: String, nullableString: Option[String])
case class TestOutputRow(id: Int, mandatoryString: String, nullableString: Option[String], doneUpper: String)
object TestOutputRow {
  def apply(input: TestInputRow, doneUpper: String): TestOutputRow = TestOutputRow(input.id, input.mandatoryString, input.nullableString, doneUpper)
}

class UppercaseCustomConformanceRuleSuite extends AnyFunSuite with SparkTestBase with MockitoSugar with HadoopFsTestBase {
  import spark.implicits._

  implicit val progArgs: ConformanceConfig = ConformanceConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: MenasDAO = mock[MenasDAO] // you may have to hard-code your own implementation here (if not working with menas)


  val experimentalMR = true
  val isCatalystWorkaroundEnabled = true
  val enableCF: Boolean = false

  test("Golden flow") {
    val input: Seq[TestInputRow] = Seq(
      TestInputRow(1, "Hello world", Some("What a beautiful place")),
      TestInputRow(4, "One Ring to rule them all", Some("One Ring to find them")),
      TestInputRow(9, "ALREADY CAPS", None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test golden flow",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        UppercaseCustomConformanceRule(order = 0, outputColumn = "doneUpper", controlCheckpoint = false, inputColumn = "mandatoryString")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: Seq[TestOutputRow] = outputData.as[TestOutputRow].collect().toSeq
    val expected: Seq[TestOutputRow] = (input zip Seq("HELLO WORLD", "ONE RING TO RULE THEM ALL", "ALREADY CAPS")).map(x => TestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Integer value") {
    val input: Seq[TestInputRow] = Seq(
      TestInputRow(1, "Hello world", Some("What a beautiful place")),
      TestInputRow(4, "One Ring to rule them all", Some("One Ring to find them")),
      TestInputRow(9, "ALREADY CAPS", None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test integer value",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        UppercaseCustomConformanceRule(order = 0, outputColumn = "doneUpper", controlCheckpoint = false, inputColumn = "id")
      )
    )

    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: Seq[TestOutputRow] = outputData.as[TestOutputRow].collect().toSeq
    val expected: Seq[TestOutputRow] = (input zip Seq("1", "4", "9")).map(x => TestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Null value") {
    val input: Seq[TestInputRow] = Seq(
      TestInputRow(1, "Hello world", Some("What a beautiful place")),
      TestInputRow(4, "One Ring to rule them all", Some("One Ring to find them")),
      TestInputRow(9, "ALREADY CAPS", None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test Null value",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        UppercaseCustomConformanceRule(order = 0, outputColumn = "doneUpper", controlCheckpoint = false, inputColumn = "nullableString")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[TestOutputRow] = outputData.as[TestOutputRow].collect().toList
    val expected: List[TestOutputRow] = (input zip Seq("WHAT A BEAUTIFUL PLACE", "ONE RING TO FIND THEM", null)).map(x => TestOutputRow(x._1, x._2)).toList

    assert(output === expected)
  }
}
