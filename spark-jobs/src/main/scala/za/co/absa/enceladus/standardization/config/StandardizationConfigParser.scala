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
  def withRowTag(value: Option[String] = None): R
  def withCsvDelimiter(value: Option[String] = None): R
  def withCsvHeader(value: Option[Boolean] = Some(false)): R
  def withCsvQuote(value: Option[String] = None): R
  def withCsvEscape(value: Option[String] = None): R
  def withCobolOptions(value: Option[CobolOptions] = None): R
  def withFixedWidthTrimValues(value: Option[Boolean] = None): R
  def withRawPathOverride(value: Option[String]): R
  def withFailOnInputNotPerSchema(value: Boolean): R
  def withFixedWidthTreatEmptyValuesAsNulls(value: Option[Boolean] = None): R
  def withFixedWidthNullValue(value: Option[String] = None): R

  def rawFormat: String
  def charset: Option[String]
  def rowTag: Option[String]
  def csvDelimiter: Option[String]
  def csvHeader: Option[Boolean]
  def csvQuote: Option[String]
  def csvEscape: Option[String]
  def cobolOptions: Option[CobolOptions]
  def fixedWidthTrimValues: Option[Boolean]
  def rawPathOverride: Option[String]
  def failOnInputNotPerSchema: Boolean
  def fixedWidthTreatEmptyValuesAsNulls: Option[Boolean]
  def fixedWidthNullValue: Option[String]
}

object StandardizationConfigParser {

  //scalastyle:off method.length the length is legit for parsing input paramters
  def standardizationParser[R <: StandardizationConfigParser[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
      help("help"),

      opt[String]('f', "raw-format").required().action((value, config) => {
        config.withRawFormat(value.toLowerCase())
      }).text("format of the raw data (csv, xml, parquet, fixed-width, etc.)"),

      opt[String]("charset").optional().action((value, config) =>
        config.withCharset(Some(value))).text("use the specific charset (default is UTF-8)"),

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

      opt[String]("null-value").optional()
        .action((value, config) => config.withFixedWidthNullValue(Some(value)))
        .text("""For FixedWidth file format. Sets the representation of a null value. Defaults is ""."""),

      checkConfig(checkConfigX(_, builder))
    )
  }
  //scalastyle:on method.length

  private val formatsSupportingCharset = List("xml", "csv", "json", "cobol", "fixed-width")

  private def typicalError(field: String, format: String): String = {
    s"The $field option is supported only for $format format"
  }

  private def checkCharset[R <: StandardizationConfigParser[R]](config: R): List[String] = {
    if (!formatsSupportingCharset.contains(config.rawFormat) && config.charset.isDefined) {
      List(typicalError("--charset", "CSV, JSON, XML, COBOL and FixedWidth"))
    } else {
      List.empty
    }
  }

  private def checkXMLFields[R <: StandardizationConfigParser[R]](config: R): List[String] = {
    if (config.rowTag.isDefined && config.rawFormat != "xml") {
      List(typicalError("--row-tag", "XML raw data"))
    } else {
      List.empty
    }
  }

  private def checkCSVFields[R <: StandardizationConfigParser[R]](config: R): List[String] = {
    def csvFieldsThatShouldNotBePresent(config: R): List[String] = {
      val format = "CSV"
      val definedFields = Map(
        typicalError("--delimiter", format) -> config.csvDelimiter.isDefined,
        typicalError("--escape", format) -> config.csvEscape.isDefined,
        typicalError("--header", s"$format raw data") -> config.csvHeader.contains(true),
        typicalError("--quote", format) -> config.csvQuote.isDefined
      )
      definedFields.filter { case (_, value) => value }.keys.toList
    }

    if (config.rawFormat == "csv") {
      List.empty
    } else {
      csvFieldsThatShouldNotBePresent(config)
    }
  }

  private def checkCobolFields[R <: StandardizationConfigParser[R]](config: R): Seq[String] = {
    def cobolFieldsThatShouldNotBePresent(cobolOptions: CobolOptions): List[String] = {
      val format = "COBOL"
      val definedFields = Map(
        typicalError("--copybook", format) -> (cobolOptions.copybook != ""),
        typicalError("--cobol-encoding", format) -> cobolOptions.encoding.isDefined,
        typicalError("--is-xcom", format) -> cobolOptions.isXcom,
        typicalError("--is-text", format) -> cobolOptions.isText
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
    def fixedWidthFieldsThatShouldNotBePresent(config: R): List[String] = {
      val format = "FixedWidth"
      val definedFields = Map(
        typicalError("--trimValues", format) -> config.fixedWidthTrimValues.isDefined,
        typicalError("--empty-values-as-nulls", format) -> config.fixedWidthTreatEmptyValuesAsNulls.isDefined,
        typicalError("--null-value", format) -> config.fixedWidthNullValue.isDefined
      )
      definedFields.filter { case (_, value) => value }.keys.toList
    }

    if (config.rawFormat == "fixed-width") {
      List.empty
    } else {
      fixedWidthFieldsThatShouldNotBePresent(config)
    }
  }

  private def checkConfigX[R <: StandardizationConfigParser[R]](config: R, builder: OParserBuilder[R]): Either[String, Unit] = {
    val allErrors:List[String] = checkCharset(config) ++
      checkXMLFields(config) ++
      checkCSVFields(config) ++
      checkCobolFields(config) ++
      checkFixedWidthFields(config)

    if (allErrors.isEmpty) {
      builder.success
    } else {
      builder.failure(allErrors.mkString("\n"))
    }
  }

}
