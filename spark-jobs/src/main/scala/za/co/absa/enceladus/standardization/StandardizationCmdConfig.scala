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

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.JobCmdConfig
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 * Even if a field is mandatory it needs a default value.
 */
case class StandardizationCmdConfig(rawFormat: String = "xml",
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
                                    persistStorageLevel: Option[StorageLevel] = None
                                   )
  extends StandardizationConfig[StandardizationCmdConfig] with JobCmdConfig[StandardizationCmdConfig]{
  override def withRawFormat(value: String): StandardizationCmdConfig = copy(rawFormat = value)
  override def withCharset(value: Option[String]): StandardizationCmdConfig = copy(charset = value)
  override def withRowTag(value: Option[String]): StandardizationCmdConfig = copy(rowTag = value)
  override def withCsvDelimiter(value: Option[String]): StandardizationCmdConfig = copy(csvDelimiter = value)
  override def withCsvHeader(value: Option[Boolean]): StandardizationCmdConfig = copy(csvHeader = value)
  override def withCsvQuote(value: Option[String]): StandardizationCmdConfig = copy(csvQuote = value)
  override def withCsvEscape(value: Option[String]): StandardizationCmdConfig = copy(csvEscape = value)
  override def withCobolOptions(value: Option[CobolOptions]): StandardizationCmdConfig = copy(cobolOptions = value)
  override def withFixedWidthTrimValues(value: Option[Boolean]): StandardizationCmdConfig = copy(fixedWidthTrimValues = value)
  override def withRawPathOverride(value: Option[String]): StandardizationCmdConfig = copy(rawPathOverride = value)
  override def withFailOnInputNotPerSchema(value: Boolean): StandardizationCmdConfig = copy(failOnInputNotPerSchema = value)

  override def withDatasetName(value: String): StandardizationCmdConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): StandardizationCmdConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): StandardizationCmdConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): StandardizationCmdConfig = copy(reportVersion = value)
  override def withMenasCredentialsFactory(value: MenasCredentialsFactory): StandardizationCmdConfig = copy(menasCredentialsFactory = value)
  override def withPerformanceMetricsFile(value: Option[String]): StandardizationCmdConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): StandardizationCmdConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): StandardizationCmdConfig = copy(persistStorageLevel = value)
}

object StandardizationCmdConfig {

  def getCmdLineArguments(args: Array[String]): StandardizationCmdConfig = {
    //Mutually exclusive parameters don't work in scopt 4
    if (args.contains("--menas-credentials-file") && args.contains("--menas-auth-keytab")) {
      println("ERROR: Only one authentication method is allowed at a time")
      System.exit(1)
    }

    val optionCmd = OParser.parse(standardizationJobParser, args, StandardizationCmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  val standardizationJobParser: OParser[_, StandardizationCmdConfig] = {
    val builder = OParser.builder[StandardizationCmdConfig]
    import builder._
    OParser.sequence(
      programName("Standardization Job"),
      head("Standardization", ""),
      StandardizationConf.standardizationParser,
      JobCmdConfig.jobConfigParser
    )
  }
}
