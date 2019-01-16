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

package za.co.absa.enceladus.standardization

import scopt.OptionParser
import za.co.absa.enceladus.utils.menas.MenasCredentials

import scala.util.matching.Regex

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  * Even if a field is mandatory it needs a default value.
  */
case class CmdConfig(datasetName: String = "",
                     datasetVersion: Int = 1,
                     reportDate: String = "",
                     reportVersion: Int = 1,
                     rawFormat: String = "xml",
                     menasCredentials: MenasCredentials = MenasCredentials("", ""),
                     rowTag: Option[String] = None,
                     csvDelimiter: Option[String] = None,
                     csvHeader: Option[Boolean] = Some(false),
                     fixedWidthTrimValues: Option[Boolean] = Some(false),
                     performanceMetricsFile: Option[String] = None,
                     rawPathOverride: Option[String] = None,
                     folderPrefix: Option[String] = None)

object CmdConfig {

  def getCmdLineArguments(args: Array[String]): CmdConfig = {
    val parser = new CmdParser("spark-submit [spark options] StandardizationBundle.jar")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[CmdConfig](programName) {
    head("\nStandardization", "")
    var rawFormat: Option[String] = None

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

    opt[String]('m', "menas-credentials-path").required().action((path, config) =>
      config.copy(menasCredentials = MenasCredentials.fromFile(path)))
      .text("Path to Menas credentials config file")

    opt[Int]('r', "report-version").action((value, config) =>
      config.copy(reportVersion = value)).text("Report version")
      .validate(value =>
        if (value > 0) success
        else failure("Option --report-version must be >0"))

    opt[String]('f', "raw-format").required().action((value, config) => {
      rawFormat = Some(value)
      config.copy(rawFormat = value)
    }).text("format of the raw data (csv, xml, parquet,fixed-width, etc.)")


    opt[String]("row-tag").optional().action((value, config) =>
      config.copy(rowTag = Some(value))).text("use the specific row tag instead of 'ROW' for XML format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("xml"))
          success
        else
          failure("The --row-tag option is supported only for XML raw data format")
      )

    opt[String]("delimiter").optional().action((value, config) =>
      config.copy(csvDelimiter = Some(value))).text("use the specific delimiter instead of ',' for CSV format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv"))
          success
        else
          failure("The --delimiter option is supported only for CSV raw data format")
      )
    // no need for validation for boolean since scopt itself will do
    opt[Boolean]("header").optional().action((value, config) =>
      config.copy(csvHeader = Some(value))).text("use the header option to consider CSV header")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv"))
          success
        else
          failure("The --header option is supported only for CSV ")
      )
    opt[Boolean]("trimValues").optional().action((value, config) =>
      config.copy(fixedWidthTrimValues = Some(value))).text("use --trimValues option to trim values in  fixed width file")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("fixed-width"))
          success
        else
          failure("The --trimValues option is supported only for fixed-width files ")
      )

    opt[String]("performance-file").optional().action((value, config) =>
      config.copy(performanceMetricsFile = Some(value))).text("produce a performance metrics file at the given location (local filesystem)")

    opt[String]("debug-set-raw-path").optional().hidden().action((value, config) =>
      config.copy(rawPathOverride = Some(value))).text("override the path of the raw data (used internally for performance tests)")

    opt[String]("folder-prefix").optional().action((value, config) =>
      config.copy(folderPrefix = Some(value))).text("Adds a folder prefix before the date tokens")

    help("help").text("prints this usage text")
  }

}
