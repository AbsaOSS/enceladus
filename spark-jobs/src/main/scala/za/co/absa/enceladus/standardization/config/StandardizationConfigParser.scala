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

package za.co.absa.enceladus.standardization.config

import scopt.{OParser, OParserBuilder}
import za.co.absa.enceladus.common.config.JobConfigParser
import za.co.absa.enceladus.standardization.CobolOptions

trait StandardizationConfigParser[R] extends JobConfigParser[R] {
  def withRawFormat(value: String): R
  def withCharset(value: Option[String] = None): R
  def withNullValue(value: Option[String] = None): R
  def withRowTag(value: Option[String] = None): R
  def withCsvDelimiter(value: Option[String] = None): R
  def withCsvHeader(value: Option[Boolean] = Some(false)): R
  def withCsvQuote(value: Option[String] = None): R
  def withCsvEscape(value: Option[String] = None): R
  def withCsvIgnoreLeadingWhiteSpace(value: Option[Boolean] = None): R
  def withCsvIgnoreTrailingWhiteSpace(value: Option[Boolean] = None): R
  def withCobolOptions(value: Option[CobolOptions] = None): R
  def withFixedWidthTrimValues(value: Option[Boolean] = None): R
  def withRawPathOverride(value: Option[String]): R
  def withFailOnInputNotPerSchema(value: Boolean): R
  def withFixedWidthTreatEmptyValuesAsNulls(value: Option[Boolean] = None): R

  def rawFormat: String
  def charset: Option[String]
  def nullValue: Option[String]
  def rowTag: Option[String]
  def csvDelimiter: Option[String]
  def csvHeader: Option[Boolean]
  def csvQuote: Option[String]
  def csvEscape: Option[String]
  def csvIgnoreLeadingWhiteSpace: Option[Boolean]
  def csvIgnoreTrailingWhiteSpace: Option[Boolean]
  def cobolOptions: Option[CobolOptions]
  def fixedWidthTrimValues: Option[Boolean]
  def rawPathOverride: Option[String]
  def failOnInputNotPerSchema: Boolean
  def fixedWidthTreatEmptyValuesAsNulls: Option[Boolean]
}

object StandardizationConfigParser {

  private val csvFormatName = "CSV"
  private val cobolFormatName = "COBOL"
  private val fixedWidthFormatName = "FixedWidth"
  private val xmlFormatName = "XML"
  private val jsonFormatName = "JSON"


  //scalastyle:off method.length the length is legit for parsing input paramters
  def standardizationParser[R <: StandardizationConfigParser[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
      opt[String]('f', "raw-format").required().action((value, config) => {
        config.withRawFormat(value.toLowerCase())
      }).text("format of the raw data (csv, xml, parquet, fixed-width, etc.)"),

      opt[String]("charset").optional().action((value, config) =>
        config.withCharset(Some(value))).text("use the specific charset (default is UTF-8)"),

      opt[String]("null-value").optional()
        .action((value, config) => config.withNullValue(Some(value)))
        .text(s"For $csvFormatName and $fixedWidthFormatName file format. Sets the representation of a null value. Defaults is empty string."), //scalastyle:ignore maxLineLength

      opt[String]("row-tag").optional().action((value, config) =>
        config.withRowTag(Some(value))).text("use the specific row tag instead of 'ROW' for XML format"),

      opt[String]("delimiter").optional().action((value, config) =>
        config.withCsvDelimiter(Some(value))).text("use the specific delimiter instead of ',' for CSV format"),

      opt[String]("csv-quote").optional().action((value, config) =>
        config.withCsvQuote(Some(value)))
        .text("use the specific quote character for creating CSV fields that may contain delimiter character(s) (default is '\"')"),

      opt[String]("csv-escape").optional().action((value, config) =>
        config.withCsvEscape(Some(value)))
        .text("use the specific escape character for CSV fields (default is '\\')"),

      opt[Boolean]("csv-ignore-leading-white-space").optional().action((value, config) =>
        config.withCsvIgnoreLeadingWhiteSpace(Some(value)))
        .text("ignore leading whitespaces for each column"),

      opt[Boolean]("csv-ignore-trailing-white-space").optional().action((value, config) =>
        config.withCsvIgnoreTrailingWhiteSpace(Some(value)))
        .text("ignore trailing whitespaces for each column"),

      // no need for validation for boolean since scopt itself will do
      opt[Boolean]("header").optional().action((value, config) =>
        config.withCsvHeader(Some(value))).text("use the header option to consider CSV header"),

      opt[Boolean]("trimValues").optional().action((value, config) =>
        config.withFixedWidthTrimValues(Some(value))).text("use --trimValues option to trim values in  fixed width file"),

      opt[Boolean]("strict-schema-check").optional().action((value, config) =>
        config.withFailOnInputNotPerSchema(value))
        .text("use --strict-schema-check option to fail or proceed over rows not adhering to the schema (with error in errCol)"),

      opt[String]("copybook").optional().action((value, config) => {
        val newOptions = config.cobolOptions match {
          case Some(a) => Some(a.copy(copybook = value))
          case None => Some(CobolOptions(value))
        }
        config.withCobolOptions(newOptions)

      }).text("Path to a copybook for COBOL data format"),

      opt[Boolean]("is-xcom").optional().action((value, config) => {
        val newOptions = config.cobolOptions match {
          case Some(a) => Some(a.copy(isXcom = value))
          case None => Some(CobolOptions(isXcom = value))
        }
        config.withCobolOptions(newOptions)
      }).text("Does a mainframe file in COBOL format contain XCOM record headers"),

      opt[Boolean]("cobol-is-text").optional().action((value, config) => {
        val newOptions = config.cobolOptions match {
          case Some(a) => Some(a.copy(isText = value))
          case None => Some(CobolOptions(isText = value))
        }
        config.withCobolOptions(newOptions)
      }).text("Specifies if the mainframe file is ASCII text file"),

      opt[String]("cobol-encoding").optional().action((value, config) => {
        val newOptions = config.cobolOptions match {
          case Some(a) => Some(a.copy(encoding = Option(value)))
          case None => Some(CobolOptions(encoding = Option(value)))
        }
        config.withCobolOptions(newOptions)
      }).text("Specify encoding of mainframe files (ascii or ebcdic)"),

      opt[String]("cobol-trimming-policy").optional().action((value, config) => {
        val newOptions = config.cobolOptions match {
          case Some(a) => Some(a.copy(trimmingPolicy = Option(value)))
          case None => Some(CobolOptions(trimmingPolicy = Option(value)))
        }
        config.withCobolOptions(newOptions)
      }).text("Specify string trimming policy for mainframe files (none, left, right, both)"),

      opt[String]("debug-set-raw-path").optional().hidden().action((value, config) =>
        config.withRawPathOverride(Some(value)))
        .text("override the path of the raw data (used internally for performance tests)"),

      opt[Boolean]("empty-values-as-nulls").optional()
        .action((value, config) => config.withFixedWidthTreatEmptyValuesAsNulls(Some(value)))
        .text("For FixedWidth file format. Treats empty values as null. Default is false"),

      checkConfig(checkConfigX(_, builder))
    )
  }
  //scalastyle:on method.length

  private val formatsSupportingCharset = List("xml", "csv", "json", "cobol", "fixed-width")

  private def unsupportedOptionError(option: String, format: String): String = {
    unsupportedOptionError(option, Seq(format))
  }

  private def unsupportedOptionError(option: String, formats: Seq[String]): String = {
    def mkErrorMessage(format: String, s: String = ""): String = s"The $option option is supported only for $format format$s"

    def mkErrorMessageForMoreFormats(allFormats: Seq[String]): String = {
      val revertedFormats = allFormats.reverse
      val format = revertedFormats.tail.reverse.mkString(", ") + " and " + revertedFormats.head
      mkErrorMessage(format, "s")
    }

    formats match {
      case Seq()       => ""
      case Seq(format) => mkErrorMessage(format)
      case _           => mkErrorMessageForMoreFormats(formats)
    }
  }

  private def checkCharset[R <: StandardizationConfigParser[R]](config: R): List[String] = {
    if (!formatsSupportingCharset.contains(config.rawFormat) && config.charset.isDefined) {
      List(unsupportedOptionError("--charset", Seq(csvFormatName, jsonFormatName, xmlFormatName, cobolFormatName, fixedWidthFormatName)))
    } else {
      List.empty
    }
  }

  private def checkXMLFields[R <: StandardizationConfigParser[R]](config: R): List[String] = {
    if (config.rowTag.isDefined && config.rawFormat != "xml") {
      List(unsupportedOptionError("--row-tag", xmlFormatName))
    } else {
      List.empty
    }
  }

  private def checkCSVFields[R <: StandardizationConfigParser[R]](config: R): Seq[String] = {
    if (config.rawFormat == "csv") {
      Seq.empty
    } else {
      Seq (
        config.csvDelimiter.map(_ => unsupportedOptionError("--delimiter", csvFormatName)),
        config.csvEscape.map(_ => unsupportedOptionError("--escape", csvFormatName)),
        config.csvHeader.map(_ => unsupportedOptionError("--header", csvFormatName)),
        config.csvQuote.map(_ => unsupportedOptionError("--quote", csvFormatName)),
        config.csvIgnoreLeadingWhiteSpace.map(_ => unsupportedOptionError("--csv-ignore-leading-white-space", csvFormatName)),
        config.csvIgnoreTrailingWhiteSpace.map(_ => unsupportedOptionError("--csv-ignore-trailing-white-space", csvFormatName))
      ).flatten
    }
  }

  private def checkCobolFields[R <: StandardizationConfigParser[R]](config: R): Seq[String] = {
    def cobolFieldsThatShouldNotBePresent(cobolOptions: CobolOptions): List[String] = {
      val definedFields = Map(
        unsupportedOptionError("--copybook", cobolFormatName) -> (cobolOptions.copybook != ""),
        unsupportedOptionError("--cobol-encoding", cobolFormatName) -> cobolOptions.encoding.isDefined,
        unsupportedOptionError("--is-xcom", cobolFormatName) -> cobolOptions.isXcom,
        unsupportedOptionError("--is-text", cobolFormatName) -> cobolOptions.isText
      )
      definedFields.filter { case (_, value) => value }.keys.toList
    }


    if (config.rawFormat == "cobol") {
      List.empty
    } else {
      config.cobolOptions
        .map(cobolFieldsThatShouldNotBePresent)
        .getOrElse(List.empty)
    }
  }

  private def checkFixedWidthFields[R <: StandardizationConfigParser[R]](config: R): Seq[String] = {
    if (config.rawFormat == "fixed-width") {
      Seq.empty
    } else {
      Seq(
        config.fixedWidthTrimValues.map(_ => unsupportedOptionError("--trimValues", fixedWidthFormatName)),
        config.fixedWidthTreatEmptyValuesAsNulls.map(_ => unsupportedOptionError("--empty-values-as-nulls", fixedWidthFormatName))
      ).flatten
    }
  }

  private def checkCSVAndFixedWidthFields[R <: StandardizationConfigParser[R]](config: R): Seq[String] = {
    if ((config.rawFormat == "csv") || (config.rawFormat == "fixed-width")) {
      List.empty
    } else {
      config.nullValue.map(_ => unsupportedOptionError("--null-value", Seq(csvFormatName, fixedWidthFormatName))).toSeq
    }
  }

  private def checkConfigX[R <: StandardizationConfigParser[R]](config: R, builder: OParserBuilder[R]): Either[String, Unit] = {
    val allErrors:List[String] = checkCharset(config) ++
      checkXMLFields(config) ++
      checkCSVFields(config) ++
      checkCobolFields(config) ++
      checkFixedWidthFields(config) ++
      checkCSVAndFixedWidthFields(config)

    if (allErrors.isEmpty) {
      builder.success
    } else {
      builder.failure(allErrors.mkString("\n"))
    }
  }

}
