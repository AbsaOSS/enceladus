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
import za.co.absa.enceladus.standardization.StdCmdConfig.stepName

case class StdConfig(rawFormat: String = "xml",
                     charset: Option[String] = None,
                     rowTag: Option[String] = None,
                     csvDelimiter: Option[String] = None,
                     csvHeader: Option[Boolean] = Some(false),
                     csvQuote: Option[String] = None,
                     csvEscape: Option[String] = None,
                     cobolOptions: Option[CobolOptions] = None,
                     fixedWidthTrimValues: Option[Boolean] = Some(false),
                     rawPathOverride: Option[String] = None,
                     failOnInputNotPerSchema: Boolean = false)

object StdConfig {
  def getCmdLineArguments(args: Array[String]): StdConfig = {
    val parser = new CmdParser(s"spark-submit [spark options] ${stepName}Bundle.jar")

    val optionCmd = parser.parse(args, StdConfig())
    optionCmd.getOrElse(StdConfig())
  }

  private class CmdParser(programName: String) extends OptionParser[StdConfig](programName) {
    override def errorOnUnknownArgument: Boolean = false
    override def reportWarning(msg: String): Unit = {}

    head("\nStandardization", "")
    var rawFormat: Option[String] = None

    opt[String]('f', "raw-format").required().action((value, config) => {
      rawFormat = Some(value)
      config.copy(rawFormat = value)
    }).text("format of the raw data (csv, xml, parquet,fixed-width, etc.)")

    opt[String]("charset").optional().action((value, config) =>
      config.copy(charset = Some(value))).text("use the specific charset (default is UTF-8)")
      .validate(value =>
        if (rawFormat.isDefined &&
          (rawFormat.get.equalsIgnoreCase("xml") ||
            rawFormat.get.equalsIgnoreCase("csv") ||
            rawFormat.get.equalsIgnoreCase("json") ||
            rawFormat.get.equalsIgnoreCase("cobol"))) {
          success
        } else {
          failure("The --charset option is supported only for CSV, JSON, XML and COBOL")
        })

    opt[String]("row-tag").optional().action((value, config) =>
      config.copy(rowTag = Some(value))).text("use the specific row tag instead of 'ROW' for XML format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("xml")) {
          success
        } else {
          failure("The --row-tag option is supported only for XML raw data format")
        })

    opt[String]("delimiter").optional().action((value, config) =>
      config.copy(csvDelimiter = Some(value))).text("use the specific delimiter instead of ',' for CSV format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --delimiter option is supported only for CSV raw data format")
        })

    opt[String]("csv-quote").optional().action((value, config) =>
      config.copy(csvQuote = Some(value)))
      .text("use the specific quote character for creating CSV fields that may contain delimiter character(s) (default is '\"')")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --csv-quote option is supported only for CSV raw data format")
        })

    opt[String]("csv-escape").optional().action((value, config) =>
      config.copy(csvEscape = Some(value)))
      .text("use the specific escape character for CSV fields (default is '\\')")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --csv-escape option is supported only for CSV raw data format")
        })

    // no need for validation for boolean since scopt itself will do
    opt[Boolean]("header").optional().action((value, config) =>
      config.copy(csvHeader = Some(value))).text("use the header option to consider CSV header")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --header option is supported only for CSV ")
        })

    opt[Boolean]("trimValues").optional().action((value, config) =>
      config.copy(fixedWidthTrimValues = Some(value))).text("use --trimValues option to trim values in  fixed width file")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("fixed-width")) {
          success
        } else {
          failure("The --trimValues option is supported only for fixed-width files ")
        })

    opt[Boolean]("strict-schema-check").optional().action((value, config) =>
      config.copy(failOnInputNotPerSchema = value))
      .text("use --strict-schema-check option to fail or proceed over rows not adhering to the schema (with error in errCol)")

    processCobolCmdOptions()

    opt[String]("debug-set-raw-path").optional().hidden().action((value, config) =>
      config.copy(rawPathOverride = Some(value))).text("override the path of the raw data (used internally for performance tests)")

    help("help").text("prints this usage text")

    private def processCobolCmdOptions(): Unit = {
      opt[String]("copybook").optional().action((value, config) => {
        config.copy(cobolOptions = cobolSetCopybook(config.cobolOptions, value))
      }).text("Path to a copybook for COBOL data format")
        .validate(value =>
          if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("cobol")) {
            success
          } else {
            failure("The --copybook option is supported only for COBOL data format")
          }
        )

      opt[Boolean]("is-xcom").optional().action((value, config) => {
        config.copy(cobolOptions = cobolSetIsXcom(config.cobolOptions, value))
      }).text("Does a mainframe file in COBOL format contain XCOM record headers")
        .validate(value =>
          if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("cobol")) {
            success
          } else {
            failure("The --is-xcom option is supported only for COBOL data format")
          })

      opt[String]("cobol-encoding").optional().action((value, config) => {
        config.copy(cobolOptions = cobolSetEncoding(config.cobolOptions, value))
      }).text("Specify encoding of mainframe files (ascii or ebcdic)")
        .validate(value =>
          if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("cobol")) {
            success
          } else {
            failure("The --cobol-encoding option is supported only for COBOL data format")
          })

      opt[String]("cobol-trimming-policy").optional().action((value, config) => {
        config.copy(cobolOptions = cobolSetTrimmingPolicy(config.cobolOptions, value))
      }).text("Specify string trimming policy for mainframe files (none, left, right, both)")
        .validate(value =>
          if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("cobol")) {
            success
          } else {
            failure("The --cobol-trimming-policy option is supported only for COBOL data format")
          })
    }

    private def cobolSetCopybook(cobolOptions: Option[CobolOptions], newCopybook: String): Option[CobolOptions] = {
      cobolOptions match {
        case Some(a) => Some(a.copy(copybook = newCopybook))
        case None => Some(CobolOptions(newCopybook))
      }
    }

    private def cobolSetIsXcom(cobolOptions: Option[CobolOptions], newIsXCom: Boolean): Option[CobolOptions] = {
      cobolOptions match {
        case Some(a) => Some(a.copy(isXcom = newIsXCom))
        case None => Some(CobolOptions(isXcom = newIsXCom))
      }
    }

    private def cobolSetEncoding(cobolOptions: Option[CobolOptions], newEncoding: String): Option[CobolOptions] = {
      cobolOptions match {
        case Some(a) => Some(a.copy(encoding = Option(newEncoding)))
        case None => Some(CobolOptions(encoding = Option(newEncoding)))
      }
    }

    private def cobolSetTrimmingPolicy(cobolOptions: Option[CobolOptions], newTrimmingPolicy: String): Option[CobolOptions] = {
      cobolOptions match {
        case Some(a) => Some(a.copy(trimmingPolicy = Option(newTrimmingPolicy)))
        case None => Some(CobolOptions(trimmingPolicy = Option(newTrimmingPolicy)))
      }
    }
  }
}
