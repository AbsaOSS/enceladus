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
import za.co.absa.enceladus.common.config.JobConfig
import za.co.absa.enceladus.conformance.config.{ConformanceConfig, ConformanceConfigInstance}
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}
import za.co.absa.enceladus.standardization.CobolOptions
import za.co.absa.enceladus.standardization.config.{StandardizationConfig, StandardizationConfigInstance}


case class StdConformanceConfigInstance(datasetName: String = "",
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
                                        keytabFile: Option[String] = None
                                  ) extends StandardizationConfig[StdConformanceConfigInstance]
  with ConformanceConfig[StdConformanceConfigInstance]{

  override def withPublishPathOverride(value: Option[String]): StdConformanceConfigInstance = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): StdConformanceConfigInstance = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): StdConformanceConfigInstance = copy(isCatalystWorkaroundEnabled = value)
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): StdConformanceConfigInstance = copy(autocleanStandardizedFolder = value)
  override def withDatasetName(value: String): StdConformanceConfigInstance = copy(datasetName = value)
  override def withDatasetVersion(value: Int): StdConformanceConfigInstance = copy(datasetVersion = value)
  override def withReportDate(value: String): StdConformanceConfigInstance = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): StdConformanceConfigInstance = copy(reportVersion = value)
  override def withPerformanceMetricsFile(value: Option[String]): StdConformanceConfigInstance = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): StdConformanceConfigInstance = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): StdConformanceConfigInstance = copy(persistStorageLevel = value)

  override def withRawFormat(value: String): StdConformanceConfigInstance = copy(rawFormat = value)
  override def withCharset(value: Option[String]): StdConformanceConfigInstance = copy(charset = value)
  override def withRowTag(value: Option[String]): StdConformanceConfigInstance = copy(rowTag = value)
  override def withCsvDelimiter(value: Option[String]): StdConformanceConfigInstance = copy(csvDelimiter = value)
  override def withCsvHeader(value: Option[Boolean]): StdConformanceConfigInstance = copy(csvHeader = value)
  override def withCsvQuote(value: Option[String]): StdConformanceConfigInstance = copy(csvQuote = value)
  override def withCsvEscape(value: Option[String]): StdConformanceConfigInstance = copy(csvEscape = value)
  override def withCobolOptions(value: Option[CobolOptions]): StdConformanceConfigInstance = copy(cobolOptions = value)
  override def withFixedWidthTrimValues(value: Option[Boolean]): StdConformanceConfigInstance = copy(fixedWidthTrimValues = value)
  override def withRawPathOverride(value: Option[String]): StdConformanceConfigInstance = copy(rawPathOverride = value)
  override def withFailOnInputNotPerSchema(value: Boolean): StdConformanceConfigInstance = copy(failOnInputNotPerSchema = value)


  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StdConformanceConfigInstance = {
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): StdConformanceConfigInstance = {
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }

}

object StdConformanceConfigInstance {

  def getCmdLineArguments(args: Array[String]): StdConformanceConfigInstance = {
    //Mutually exclusive parameters don't work in scopt 4
    if (args.contains("--menas-credentials-file") && args.contains("--menas-auth-keytab")) {
      println("ERROR: Only one authentication method is allowed at a time")
      System.exit(1)
    }
    val optionCmd = OParser.parse(stdConfJobParser, args, StdConformanceConfigInstance())

    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  val stdConfJobParser: OParser[_, StdConformanceConfigInstance] = {
    val builder = OParser.builder[StdConformanceConfigInstance]
    import builder._
    OParser.sequence(
      programName("Standardization Conformance Job"),
      StandardizationConfig.standardizationParser,
      ConformanceConfig.conformanceParser,
      JobConfig.jobConfigParser
    )
  }

}

