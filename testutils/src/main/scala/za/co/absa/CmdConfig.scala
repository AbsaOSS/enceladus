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

package za.co.absa.enceladus.standardization

import scopt.OptionParser

import scala.util.matching.Regex

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scope requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class CmdConfig(datasetName: String = "",
                     datasetVersion: Int = 1,
                     stdPath: String = "",
                     refPath: String = "")

object CmdConfig {

  def getCmdLineArguments(args: Array[String]): CmdConfig = {
    val parser = new CmdParser("spark-submit [spark options] TestUtils.jar")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[CmdConfig](programName) {
    head("\nStandardization", "")

    opt[String]('D', "dataset-name").required().action((value, config) =>
      config.copy(datasetName = value)).text("Dataset name")

    opt[Int]('d', "dataset-version").required().action((value, config) =>
      config.copy(datasetVersion = value)).text("Dataset version")
      .validate(value =>
        if (value > 0) success
        else failure("Option --dataset-version must be > 0"))

    opt[String]("std-path").action((value, config) =>
      config.copy(stdPath = value)).text("Path to standardized files")

    opt[String]("ref-path").action((value, config) =>
      config.copy(refPath = value)).text("Path to referential files")

    help("help").text("prints this usage text")
  }

}
