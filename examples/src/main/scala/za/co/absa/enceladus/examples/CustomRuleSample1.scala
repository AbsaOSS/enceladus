/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.dao.{EnceladusDAO, EnceladusRestDAO}
import za.co.absa.enceladus.examples.interpreter.rules.custom.UppercaseCustomConformanceRule
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object CustomRuleSample1 {

  case class ExampleRow(id: Int, makeUpper: String, leave: String)
  case class OutputRow(id: Int, makeUpper: String, leave: String, doneUpper: String)

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CustomRuleSample1")
    .config("spark.sql.codegen.wholeStage", value = false)
    .getOrCreate()

  def main(args: Array[String]) {
    import spark.implicits._

    TimeZoneNormalizer.normalizeTimezone()
    implicit val progArgs: CmdConfig = CmdConfig() // here we may need to specify some parameters (for certain rules)
    implicit val dao: EnceladusDAO = EnceladusRestDAO // you may have to hard-code your own implementation here (if not working with menas)
    implicit val enableCF: Boolean = false

    val inputData = spark.createDataFrame(
      Seq(
        ExampleRow(1, "Hello world", "What a beautiful place"),
        ExampleRow(4, "One Ring to rule them all", "One Ring to find them"),
        ExampleRow(9, "ALREADY CAPS", "and this is lower-case")
      ))

    val conformanceDef =  Dataset(
      name = "Custom rule sample 1",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999,

      conformance = List(
        UppercaseCustomConformanceRule(order = 0, outputColumn = "doneUpper", controlCheckpoint = false, inputColumn = "makeUpper")
      )
    )

    val outputData: DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    outputData.show(false)
  }
}
