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

package za.co.absa.enceladus.standardization_conformance.config

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.config.{ConfigError, JobConfigParser}
import za.co.absa.enceladus.conformance.config.ConformanceParser
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}
import za.co.absa.enceladus.standardization.CobolOptions
import za.co.absa.enceladus.standardization.config.StandardizationParser

import scala.util.Try


case class StdConformanceConfig(datasetName: String = "",
                                datasetVersion: Int = 1,
                                reportDate: String = "",
                                reportVersion: Option[Int] = None,
                                menasCredentialsFactory: MenasCredentialsFactory = InvalidMenasCredentialsFactory,
                                performanceMetricsFile: Option[String] = None,
                                folderPrefix: Option[String] = None,
                                persistStorageLevel: Option[StorageLevel] = None,

                                publishPathOverride: Option[String] = None,
                                experimentalMappingRule: Option[Boolean] = None,
                                isCatalystWorkaroundEnabled: Option[Boolean] = None,
                                autocleanStandardizedFolder: Option[Boolean] = None,

                                rawFormat: String = "xml",
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

                                credsFile: Option[String] = None,
                                keytabFile: Option[String] = None) extends StandardizationParser[StdConformanceConfig]
  with ConformanceParser[StdConformanceConfig]{

  override def withPublishPathOverride(value: Option[String]): StdConformanceConfig = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): StdConformanceConfig = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): StdConformanceConfig = copy(isCatalystWorkaroundEnabled = value)
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): StdConformanceConfig = copy(autocleanStandardizedFolder = value)
  override def withDatasetName(value: String): StdConformanceConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): StdConformanceConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): StdConformanceConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): StdConformanceConfig = copy(reportVersion = value)
  override def withPerformanceMetricsFile(value: Option[String]): StdConformanceConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): StdConformanceConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): StdConformanceConfig = copy(persistStorageLevel = value)

  override def withRawFormat(value: String): StdConformanceConfig = copy(rawFormat = value)
  override def withCharset(value: Option[String]): StdConformanceConfig = copy(charset = value)
  override def withRowTag(value: Option[String]): StdConformanceConfig = copy(rowTag = value)
  override def withCsvDelimiter(value: Option[String]): StdConformanceConfig = copy(csvDelimiter = value)
  override def withCsvHeader(value: Option[Boolean]): StdConformanceConfig = copy(csvHeader = value)
  override def withCsvQuote(value: Option[String]): StdConformanceConfig = copy(csvQuote = value)
  override def withCsvEscape(value: Option[String]): StdConformanceConfig = copy(csvEscape = value)
  override def withCobolOptions(value: Option[CobolOptions]): StdConformanceConfig = copy(cobolOptions = value)
  override def withFixedWidthTrimValues(value: Option[Boolean]): StdConformanceConfig = copy(fixedWidthTrimValues = value)
  override def withRawPathOverride(value: Option[String]): StdConformanceConfig = copy(rawPathOverride = value)
  override def withFailOnInputNotPerSchema(value: Boolean): StdConformanceConfig = copy(failOnInputNotPerSchema = value)

  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StdConformanceConfig = {
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StdConformanceConfig = {
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
}

object StdConformanceConfig {

  def tryFromArguments(args: Array[String]): Try[StdConformanceConfig] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits._
    OParser.parse(stdConfJobParser, args, StdConformanceConfig()).toTry(ConfigError("Command line parameters error"))
  }

  def getFromArguments(args: Array[String]): StdConformanceConfig = tryFromArguments(args).get

  val stdConfJobParser: OParser[_, StdConformanceConfig] = {
    val builder = OParser.builder[StdConformanceConfig]
    import builder._
    OParser.sequence(
      programName("Standardization Conformance Job"),
      StandardizationParser.standardizationParser,
      ConformanceParser.conformanceParser,
      JobConfigParser.jobConfigParser
    )
  }

}

