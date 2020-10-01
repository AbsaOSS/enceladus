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
import za.co.absa.enceladus.conformance.config.ConformanceConfigParser
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}
import za.co.absa.enceladus.standardization.CobolOptions
import za.co.absa.enceladus.standardization.config.StandardizationConfigParser

import scala.util.Try


case class StandardizationConformanceConfig(datasetName: String = "",
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
                                            nullValue: Option[String] = None,
                                            rowTag: Option[String] = None,
                                            csvDelimiter: Option[String] = None,
                                            csvHeader: Option[Boolean] = None,
                                            csvQuote: Option[String] = None,
                                            csvEscape: Option[String] = None,
                                            cobolOptions: Option[CobolOptions] = None,
                                            fixedWidthTrimValues: Option[Boolean] = None,
                                            rawPathOverride: Option[String] = None,
                                            failOnInputNotPerSchema: Boolean = false,
                                            fixedWidthTreatEmptyValuesAsNulls: Option[Boolean] = None,

                                            credsFile: Option[String] = None,
                                            keytabFile: Option[String] = None)
  extends StandardizationConfigParser[StandardizationConformanceConfig]
  with ConformanceConfigParser[StandardizationConformanceConfig]{

  override def withPublishPathOverride(value: Option[String]): StandardizationConformanceConfig = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): StandardizationConformanceConfig = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): StandardizationConformanceConfig = {
    copy(isCatalystWorkaroundEnabled = value)
  }
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): StandardizationConformanceConfig = {
    copy(autocleanStandardizedFolder = value)
  }
  override def withDatasetName(value: String): StandardizationConformanceConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): StandardizationConformanceConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): StandardizationConformanceConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): StandardizationConformanceConfig = copy(reportVersion = value)
  override def withPerformanceMetricsFile(value: Option[String]): StandardizationConformanceConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): StandardizationConformanceConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): StandardizationConformanceConfig = copy(persistStorageLevel = value)

  override def withRawFormat(value: String): StandardizationConformanceConfig = copy(rawFormat = value)
  override def withCharset(value: Option[String]): StandardizationConformanceConfig = copy(charset = value)
  override def withNullValue(value: Option[String]): StandardizationConformanceConfig = copy(nullValue = value)
  override def withRowTag(value: Option[String]): StandardizationConformanceConfig = copy(rowTag = value)
  override def withCsvDelimiter(value: Option[String]): StandardizationConformanceConfig = copy(csvDelimiter = value)
  override def withCsvHeader(value: Option[Boolean]): StandardizationConformanceConfig = copy(csvHeader = value)
  override def withCsvQuote(value: Option[String]): StandardizationConformanceConfig = copy(csvQuote = value)
  override def withCsvEscape(value: Option[String]): StandardizationConformanceConfig = copy(csvEscape = value)
  override def withCobolOptions(value: Option[CobolOptions]): StandardizationConformanceConfig = copy(cobolOptions = value)
  override def withFixedWidthTrimValues(value: Option[Boolean]): StandardizationConformanceConfig = copy(fixedWidthTrimValues = value)
  override def withRawPathOverride(value: Option[String]): StandardizationConformanceConfig = copy(rawPathOverride = value)
  override def withFailOnInputNotPerSchema(value: Boolean): StandardizationConformanceConfig = copy(failOnInputNotPerSchema = value)
  override def withFixedWidthTreatEmptyValuesAsNulls(value: Option[Boolean]): StandardizationConformanceConfig = {
    copy(fixedWidthTreatEmptyValuesAsNulls = value)
  }

  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StandardizationConformanceConfig = {
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StandardizationConformanceConfig = {
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
}

object StandardizationConformanceConfig {

  def tryFromArguments(args: Array[String]): Try[StandardizationConformanceConfig] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits._
    OParser.parse(stdConfJobParser, args, StandardizationConformanceConfig())
      .toTry(ConfigError("Command line parameters error"))
  }

  def getFromArguments(args: Array[String]): StandardizationConformanceConfig = tryFromArguments(args).get

  val stdConfJobParser: OParser[_, StandardizationConformanceConfig] = {
    val builder = OParser.builder[StandardizationConformanceConfig]
    import builder._
    OParser.sequence(
      programName("Standardization Conformance Job"),
      StandardizationConfigParser.standardizationParser,
      ConformanceConfigParser.conformanceParser,
      JobConfigParser.jobConfigParser
    )
  }

}

