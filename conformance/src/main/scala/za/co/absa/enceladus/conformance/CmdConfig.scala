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

package za.co.absa.enceladus.conformance

import scopt.OptionParser

import scala.util.matching.Regex

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class CmdConfig(datasetName: String = "",
                     datasetVersion: Int = 1,
                     reportDate: String = "",
                     reportVersion: Int = 1,
                     performanceMetricsFile: Option[String] = None,
                     publishPathOverride: Option[String] = None,
                     folderPrefix: Option[String] = None)

object CmdConfig {

  def getCmdLineArguments(args: Array[String]): CmdConfig = {
    val parser = new CmdParser("spark-submit [spark options] ConformanceBundle.jar")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[CmdConfig](programName) {
    head("Dynamic Conformance", "")

    opt[String]('D', "dataset-name").required().action((value, config) =>
      config.copy(datasetName = value)).text("Dataset name")

    opt[Int]('d', "dataset-version").required().action((value, config) =>
      config.copy(datasetVersion = value)).text("Dataset version")
      .validate(value =>
        if (value > 0) success
        else failure("Option --dataset-version must be >0"))

    val reportDateMatcher: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r
    opt[String]('R', "report-date").required().action((value, config) =>
      config.copy(reportDate = value)).text("Report date in 'yyyy-MM-dd' format")
      .validate(value =>
        reportDateMatcher.findFirstIn(value) match {
          case None => failure(s"Match error in '$value'. Option --report-date expects a date in 'yyyy-MM-dd' format")
          case _ => success
        }
      )

    opt[Int]('r', "report-version").action((value, config) =>
      config.copy(reportVersion = value)).text("Report version")
      .validate(value =>
        if (value > 0) success
        else failure("Option --report-version must be >0"))

    opt[String]("performance-file").optional().action((value, config) =>
      config.copy(performanceMetricsFile = Some(value))).text("Produce a performance metrics file at the given location (local filesystem)")

    opt[String]("debug-set-publish-path").optional().hidden().action((value, config) =>
      config.copy(publishPathOverride = Some(value))).text("override the path of the published data (used internally for testing)")

    opt[String]("folder-prefix").optional().action((value, config) =>
      config.copy(folderPrefix = Some(value))).text("Adds a folder prefix before the infoDateColumn")

    help("help").text("prints this usage text")
  }

}
