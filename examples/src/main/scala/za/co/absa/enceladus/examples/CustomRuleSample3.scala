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

package za.co.absa.enceladus.examples

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.dao.auth.RestApiKerberosCredentials
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.examples.interpreter.rules.custom.{LPadCustomConformanceRule, UppercaseCustomConformanceRule}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.config.UrisConnectionStringParser
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object CustomRuleSample3 extends CustomRuleSampleFs {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("CustomRuleSample3")
    .config("spark.sql.codegen.wholeStage", value = false)
    .getOrCreate()
  TimeZoneNormalizer.normalizeAll(spark) //normalize the timezone of JVM and the spark session

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    val restApiBaseUrls = UrisConnectionStringParser.parse(conf.getString("enceladus.rest.uri"))
    val restApiCredentials = RestApiKerberosCredentials("user@EXAMPLE.COM", "src/main/resources/user.keytab.example")
    implicit val progArgs: ConformanceConfig = ConformanceConfig() // here we may need to specify some parameters (for certain rules)
    implicit val dao: EnceladusDAO = RestDaoFactory.getInstance(restApiCredentials, restApiBaseUrls) // you may have to hard-code your own implementation here

    val experimentalMR = true
    val isCatalystWorkaroundEnabled = true
    val enableCF: Boolean = false

    val inputData = spark.read
      .option("header", "true")
      .csv("examples/data/input/example_data.csv")

    // scalastyle:off magic.number
    val conformanceDef =  Dataset(
      name = "Custom rule sample 3",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        UppercaseCustomConformanceRule(order = 0,
                                       outputColumn = "upper",
                                       controlCheckpoint = false,
                                       inputColumn = "text_column"),
        LPadCustomConformanceRule(order = 1,
                                  outputColumn = "final",
                                  controlCheckpoint = false,
                                  inputColumn = "upper",
                                  len = 25,
                                  pad = ".")
      )
    )
    // scalastyle:on magic.number

    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val outputData: DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)

    outputData.show()
  }

}
