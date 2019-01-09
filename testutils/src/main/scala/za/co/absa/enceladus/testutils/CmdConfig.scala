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

package za.co.absa.enceladus.testutils

import scopt.OptionParser

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class CmdConfig(rawFormat: String = "xml",
                     rowTag: Option[String] = None,
                     csvDelimiter: Option[String] = None,
                     csvHeader: Option[Boolean] = Some(false),
                     fixedWidthTrimValues: Option[Boolean] = Some(false),
                     stdPath: String = "",
                     refPath: String = "",
                     outPath: String = "")

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
    head("\nDatasets Comparison", "")
    var rawFormat: Option[String] = None
    var stdPath: Option[String] = None
    var refPath: Option[String] = None
    var outPath: Option[String] = None

    opt[String]('f', "raw-format").required.action((value, config) => {
      rawFormat = Some(value)
      config.copy(rawFormat = value)
    }).text("format of the raw data (csv, xml, parquet,fixed-width, etc.)")


    opt[String]("row-tag").optional.action((value, config) =>
      config.copy(rowTag = Some(value))).text("use the specific row tag instead of 'ROW' for XML format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("xml"))
          success
        else
          failure("The --row-tag option is supported only for XML raw data format")
      )

    opt[String]("delimiter").optional.action((value, config) =>
      config.copy(csvDelimiter = Some(value))).text("use the specific delimiter instead of ',' for CSV format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv"))
          success
        else
          failure("The --delimiter option is supported only for CSV raw data format")
      )
    // no need for validation for boolean since scopt itself will do
    opt[Boolean]("header").optional.action((value, config) =>
      config.copy(csvHeader = Some(value))).text("use the header option to consider CSV header")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv"))
          success
        else
          failure("The --header option is supported only for CSV ")
      )

    opt[Boolean]("trim-values").optional.action((value, config) =>
      config.copy(fixedWidthTrimValues = Some(value))).text("use --trimValues option to trim values in  fixed width file")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("fixed-width"))
          success
        else
          failure("The --trimValues option is supported only for fixed-width files ")
      )

    opt[String]("std-path").required.action((value, config) => {
      stdPath = Some(value)
      config.copy(stdPath = value)
    }).text("Path to standardized dataset")
      .validate(value =>
        if (refPath.isDefined && refPath.get.equalsIgnoreCase(value))
            failure("std-path and ref-path can not be equal")
        else if (outPath.isDefined && outPath.get.equalsIgnoreCase(value))
            failure("std-path and out-path can not be equal")
        else
            success
        )

    opt[String]("ref-path").required.action((value, config) => {
      refPath = Some(value)
      config.copy(refPath = value)
    }).text("Path to referential dataset")
      .validate(value =>
        if (stdPath.isDefined && stdPath.get.equalsIgnoreCase(value))
          failure("ref-path and std-path can not be equal")
        else if (outPath.isDefined && outPath.get.equalsIgnoreCase(value))
          failure("ref-path and out-path can not be equal")
        else
          success
        )

    opt[String]("out-path").required.action((value, config) => {
      outPath = Some(value)
      config.copy(outPath = value)
    }).text("Path to diff output")
      .validate(value =>
        if (stdPath.isDefined && stdPath.get.equalsIgnoreCase(value))
          failure("out-path and std-path can not be equal")
        else if (refPath.isDefined && refPath.get.equalsIgnoreCase(value))
          failure("out-path and ref-path can not be equal")
        else
          success
      )

    help("help").text("prints this usage text")
  }
}
