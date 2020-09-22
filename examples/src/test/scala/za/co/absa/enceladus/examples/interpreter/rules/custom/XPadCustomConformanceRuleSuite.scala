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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.scalatest.MockitoSugar
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasKerberosCredentials
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.fs.HdfsUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

case class XPadTestInputRow(intField: Int, stringField: Option[String])
case class XPadTestOutputRow(intField: Int, stringField: Option[String], targetField: String)
object XPadTestOutputRow {
  def apply(input: XPadTestInputRow, targetField: String): XPadTestOutputRow = XPadTestOutputRow(input.intField, input.stringField, targetField)
}

class LpadCustomConformanceRuleSuite extends AnyFunSuite with SparkTestBase with MockitoSugar {
  import spark.implicits._

  implicit val progArgs: ConformanceConfig = ConformanceConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: MenasDAO = mock[MenasDAO] // you may have to hard-code your own implementation here (if not working with menas)
  implicit val fsUtils: HdfsUtils = new HdfsUtils(spark.sparkContext.hadoopConfiguration)

  val experimentalMR = true
  val isCatalystWorkaroundEnabled = true
  val enableCF: Boolean = false

  test("String values") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("Short")),
      XPadTestInputRow(2,Some("This is long")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test string values",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = 8, pad = "~")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("~~~Short", "This is long", "~~~~~~~~")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Integer value") {
    val input: Seq[XPadTestInputRow] = Seq(
      XPadTestInputRow(7, Some("Agent")),
      XPadTestInputRow(42, None),
      XPadTestInputRow(100000, Some("A lot"))
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
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "intField", len = 3, pad = "0")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: Seq[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toSeq
    val expected: Seq[XPadTestOutputRow] = (input zip Seq("007", "042", "100000")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Multicharacter pad") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("abcdefgh")),
      XPadTestInputRow(2,Some("$$$")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test multicharacter pad",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = 10, pad = "123")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("12abcdefgh", "1231231$$$", "1231231231")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Negative length") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("A")),
      XPadTestInputRow(2,Some("AAAAAAAAAAAAAAAAAAAA")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test negative length",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = -5, pad = ".")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("A", "AAAAAAAAAAAAAAAAAAAA", "")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }
}


class RpadCustomConformanceRuleSuite extends AnyFunSuite with SparkTestBase {

  import spark.implicits._

  private val conf = ConfigFactory.load()
  private val menasBaseUrls = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))
  private val meansCredentials = MenasKerberosCredentials("user@EXAMPLE.COM", "src/test/resources/user.keytab.example")
  implicit val progArgs: ConformanceConfig = ConformanceConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: MenasDAO = RestDaoFactory.getInstance(meansCredentials, menasBaseUrls) // you may have to hard-code your own implementation here (if not working with menas)
  implicit val fsUtils: HdfsUtils = new HdfsUtils(spark.sparkContext.hadoopConfiguration)

  val experimentalMR = true
  val isCatalystWorkaroundEnabled = true
  val enableCF: Boolean = false

  test("String values") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("Short")),
      XPadTestInputRow(2,Some("This is long")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test string values",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = 8, ".")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("Short...", "This is long", "........")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Integer value") {
    val input: Seq[XPadTestInputRow] = Seq(
      XPadTestInputRow(1, Some("Cent")),
      XPadTestInputRow(42, None),
      XPadTestInputRow(100000, Some("A lot"))
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
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "intField", len = 3, pad = "0")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: Seq[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toSeq
    val expected: Seq[XPadTestOutputRow] = (input zip Seq("100", "420", "100000")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Multicharacter pad") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("abcdefgh")),
      XPadTestInputRow(2,Some("$$$")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test multicharacter pad",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = 10, pad = "123")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("abcdefgh12", "$$$1231231", "1231231231")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Negative length") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("A")),
      XPadTestInputRow(2,Some("AAAAAAAAAAAAAAAAAAAA")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test negative length",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", len = -5, pad = "#")
      )
    )
    implicit val featureSwitches:FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: sql.DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("A", "AAAAAAAAAAAAAAAAAAAA", "")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }
}
