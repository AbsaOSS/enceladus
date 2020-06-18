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


import scopt.OParser

case class StandardizationConfigAcc(rawFormat: String = "xml",
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

trait StandardizationConfig[R] {
  def withRawFormat(value: String): R
  def withCharset(value: Option[String] = None): R
  def withRowTag(value: Option[String] = None): R
  def withCsvDelimiter(value: Option[String] = None): R
  def withCsvHeader(value: Option[Boolean] = Some(false)): R
  def withCsvQuote(value: Option[String] = None): R
  def withCsvEscape(value: Option[String] = None): R
  def withCobolOptions(value: Option[CobolOptions] = None): R
  def withFixedWidthTrimValues(value: Option[Boolean] = Some(false)): R
  def withRawPathOverride(value: Option[String]): R
  def withFailOnInputNotPerSchema(value: Boolean): R

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
}

object StandardizationConf {
  def standardizationParser[R <: StandardizationConfig[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
      head("\nStandardization", ""),

      opt[String]('f', "raw-format").required().action((value, config) => {
        config.withRawFormat(value)
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

      checkConfig(config => {
        def foundCsvField(config: R) = config match {
          case _ if config.csvDelimiter.isDefined => Some("--delimiter")
          case _ if config.csvEscape.isDefined => Some("--escape")
          case _ if config.csvHeader.contains(true) => Some("--header")
          case _ if config.csvQuote.isDefined => Some("--quote")
          case _ => None
        }

        def foundCobolField(cobolOptions: CobolOptions) = cobolOptions match {
          case _ if cobolOptions.copybook != "" => Some("--copybook")
          case _ if cobolOptions.encoding.isDefined => Some("--cobol-encoding")
          case _ if cobolOptions.isXcom => Some("--is-xcom")
          case _ if cobolOptions.trimmingPolicy.isDefined => Some("--cobol-trimming-policy")
          case _ => None
        }


        if (!List("xml", "csv", "json", "cobol").contains(config.rawFormat.toLowerCase) && config.charset.isDefined)
          failure("The --charset option is supported only for CSV, JSON, XML and COBOL")
        else if (config.rowTag.isDefined && !config.rawFormat.equalsIgnoreCase("xml"))
          failure("The --row-tag option is supported only for XML raw data format")
        else if (foundCsvField(config).isDefined && !config.rawFormat.equalsIgnoreCase("csv")) {
          foundCsvField(config) match {
            case Some("header") => failure("The --header option is supported only for CSV ")
            case Some(field) => failure(s"The $field option is supported only for CSV raw data format")
            case None =>
              val cobolOptions = config.cobolOptions
              if (cobolOptions.isDefined && !config.rawFormat.equalsIgnoreCase("cobol")) {
                val field = foundCobolField(cobolOptions.get)
                failure(s"The ${field} option is supported only for COBOL data format")
              } else success
          }
        } else success
      })
    )
  }
}
