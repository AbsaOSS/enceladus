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

import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.common._
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfigParser
import za.co.absa.enceladus.utils.unicode.ParameterConversion._

import scala.collection.immutable.HashMap

/**
  * Reads standardization properties from the configuration file
  */
class StandardizationPropertiesProvider {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private final val SparkCSVReaderMaxColumnsDefault: Int = 20480

  /**
   * Returns a Spark reader with all format-specific options applied.
   * Options are provided by command line parameters.
   *
   * @param cmd             Command line parameters containing format-specific options
   * @param dataset         A dataset definition
   * @param numberOfColumns (Optional) number of columns, enables reading CSV files with the number of columns
   *                        larger than Spark default
   * @return The updated dataframe reader
   */
  def getFormatSpecificReader[T](cmd: StandardizationConfigParser[T], dataset: Dataset, numberOfColumns: Int = 0)
                                (implicit spark: SparkSession, dao: MenasDAO): DataFrameReader = {
    val dfReader = spark.read.format(cmd.rawFormat)
    // applying format specific options
    val options = getCobolOptions(cmd, dataset) ++
      getGenericOptions(cmd) ++
      getXmlOptions(cmd) ++
      getCsvOptions(cmd, numberOfColumns) ++
      getFixedWidthOptions(cmd)

    // Applying all the options
    options.foldLeft(dfReader) { (df, optionPair) =>
      optionPair match {
        case (key, Some(value)) =>
          value match {
            // Handle all .option() overloads
            case StringParameter(s) => df.option(key, s)
            case BooleanParameter(b) => df.option(key, b)
            case LongParameter(l) => df.option(key, l)
            case DoubleParameter(d) => df.option(key, d)
          }
        case (_, None) => df
      }
    }
  }

  private def getGenericOptions[T](cmd: StandardizationConfigParser[T]): HashMap[String, Option[RawFormatParameter]] = {
    val mode = if (cmd.failOnInputNotPerSchema) {
      "FAILFAST"
    } else {
      "PERMISSIVE"
    }
    HashMap(
      "charset" -> cmd.charset.map(StringParameter),
      "mode" -> Option(StringParameter(mode))
    )
  }

  private def getXmlOptions[T](cmd: StandardizationConfigParser[T]): HashMap[String, Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("xml")) {
      HashMap("rowtag" -> cmd.rowTag.map(StringParameter))
    } else {
      HashMap()
    }
  }

  private def getCsvOptions[T](cmd: StandardizationConfigParser[T],
                               numberOfColumns: Int = 0): HashMap[String, Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("csv")) {
      HashMap(
        "delimiter" -> cmd.csvDelimiter.map(s => StringParameter(s.includingUnicode.includingNone)),
        "header" -> cmd.csvHeader.map(BooleanParameter),
        "quote" -> cmd.csvQuote.map(s => StringParameter(s.includingUnicode.includingNone)),
        "escape" -> cmd.csvEscape.map(s => StringParameter(s.includingUnicode.includingNone)),
        // increase the default limit on the number of columns if needed
        // default is set at org.apache.spark.sql.execution.datasources.csv.CSVOptions maxColumns
        "maxColumns" -> {
          if (numberOfColumns > SparkCSVReaderMaxColumnsDefault) Some(LongParameter(numberOfColumns)) else None
        }
      )
    } else {
      HashMap()
    }
  }

  private def getFixedWidthOptions[T](cmd: StandardizationConfigParser[T]): HashMap[String, Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      HashMap("trimValues" -> cmd.fixedWidthTrimValues.map(BooleanParameter))
    } else {
      HashMap()
    }
  }

  private def getCobolOptions[T](cmd: StandardizationConfigParser[T], dataset: Dataset)
                                (implicit dao: MenasDAO): HashMap[String, Option[RawFormatParameter]] = {
    if (cmd.rawFormat.equalsIgnoreCase("cobol")) {
      val cobolOptions = cmd.cobolOptions.getOrElse(CobolOptions())
      val isXcomOpt = if (cobolOptions.isXcom) Some(true) else None
      val isTextOpt = if (cobolOptions.isText) Some(true) else None
      val isAscii = cobolOptions.encoding.exists(_.equalsIgnoreCase("ascii"))
      // For ASCII files --charset is converted into Cobrix "ascii_charset" option
      // For EBCDIC files --charset is converted into Cobrix "ebcdic_code_page" option
      HashMap(
        getCopybookOption(cobolOptions, dataset),
        "is_xcom" -> isXcomOpt.map(BooleanParameter),
        "is_text" -> isTextOpt.map(BooleanParameter),
        "string_trimming_policy" -> cobolOptions.trimmingPolicy.map(StringParameter),
        "encoding" -> cobolOptions.encoding.map(StringParameter),
        "ascii_charset" -> cmd.charset.flatMap(charset => if (isAscii) Option(StringParameter(charset)) else None),
        "ebcdic_code_page" -> cmd.charset.flatMap(charset => if (!isAscii) Option(StringParameter(charset)) else None),
        "schema_retention_policy" -> Some(StringParameter("collapse_root"))
      )
    } else {
      HashMap()
    }
  }

  private def getCopybookOption(opts: CobolOptions, dataset: Dataset)(implicit dao: MenasDAO): (String, Option[RawFormatParameter]) = {
    val copybook = opts.copybook
    if (copybook.isEmpty) {
      log.info("Copybook location is not provided via command line - fetching the copybook attached to the schema...")
      val copybookContents = dao.getSchemaAttachment(dataset.schemaName, dataset.schemaVersion)
      log.info(s"Applying the following copybook:\n$copybookContents")
      ("copybook_contents", Option(StringParameter(copybookContents)))
    } else {
      log.info(s"Use copybook at $copybook")
      ("copybook", Option(StringParameter(copybook)))
    }
  }

}
