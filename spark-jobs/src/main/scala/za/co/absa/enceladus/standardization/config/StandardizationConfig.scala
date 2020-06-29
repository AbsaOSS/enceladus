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

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.config.{ConfigError, JobParser}
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}
import za.co.absa.enceladus.standardization.CobolOptions

import scala.util.Try

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 * Even if a field is mandatory it needs a default value.
 */
case class StandardizationConfig(rawFormat: String = "xml",
                                 charset: Option[String] = None,
                                 rowTag: Option[String] = None,
                                 csvDelimiter: Option[String] = None,
                                 csvHeader: Option[Boolean] = Some(false),
                                 csvQuote: Option[String] = None,
                                 csvEscape: Option[String] = None,
                                 cobolOptions: Option[CobolOptions] = None,
                                 fixedWidthTrimValues: Option[Boolean] = Some(false),
                                 rawPathOverride: Option[String] = None,
                                 failOnInputNotPerSchema: Boolean = false,
                                 datasetName: String = "",
                                 datasetVersion: Int = 1,
                                 reportDate: String = "",
                                 reportVersion: Option[Int] = None,
                                 menasCredentialsFactory: MenasCredentialsFactory = InvalidMenasCredentialsFactory,
                                 performanceMetricsFile: Option[String] = None,
                                 folderPrefix: Option[String] = None,
                                 persistStorageLevel: Option[StorageLevel] = None,
                                 credsFile: Option[String] = None,
                                 keytabFile: Option[String] = None
                                        )
  extends StandardizationParser[StandardizationConfig] with JobParser[StandardizationConfig]{
  override def withRawFormat(value: String): StandardizationConfig = copy(rawFormat = value)
  override def withCharset(value: Option[String]): StandardizationConfig = copy(charset = value)
  override def withRowTag(value: Option[String]): StandardizationConfig = copy(rowTag = value)
  override def withCsvDelimiter(value: Option[String]): StandardizationConfig = copy(csvDelimiter = value)
  override def withCsvHeader(value: Option[Boolean]): StandardizationConfig = copy(csvHeader = value)
  override def withCsvQuote(value: Option[String]): StandardizationConfig = copy(csvQuote = value)
  override def withCsvEscape(value: Option[String]): StandardizationConfig = copy(csvEscape = value)
  override def withCobolOptions(value: Option[CobolOptions]): StandardizationConfig = copy(cobolOptions = value)
  override def withFixedWidthTrimValues(value: Option[Boolean]): StandardizationConfig = copy(fixedWidthTrimValues = value)
  override def withRawPathOverride(value: Option[String]): StandardizationConfig = copy(rawPathOverride = value)
  override def withFailOnInputNotPerSchema(value: Boolean): StandardizationConfig = copy(failOnInputNotPerSchema = value)

  override def withDatasetName(value: String): StandardizationConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): StandardizationConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): StandardizationConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): StandardizationConfig = copy(reportVersion = value)
  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StandardizationConfig = {
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StandardizationConfig = {
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withPerformanceMetricsFile(value: Option[String]): StandardizationConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): StandardizationConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): StandardizationConfig = copy(persistStorageLevel = value)
}

object StandardizationConfig {

  def tryFromArguments(args: Array[String]): Try[StandardizationConfig] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits._
    OParser.parse(standardizationJobParser, args, StandardizationConfig()).toTry(ConfigError("Command line parameters error"))
  }

  def getFromArguments(args: Array[String]): StandardizationConfig = tryFromArguments(args).get

  private val standardizationJobParser: OParser[_, StandardizationConfig] = {
    val builder = OParser.builder[StandardizationConfig]
    import builder._
    OParser.sequence(
      programName("Standardization Job"),
      head("Standardization", ""),
      StandardizationParser.standardizationParser,
      JobParser.jobConfigParser
    )
  }
}
