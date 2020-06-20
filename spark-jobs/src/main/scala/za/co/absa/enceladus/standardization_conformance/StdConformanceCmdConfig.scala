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

package za.co.absa.enceladus.standardization_conformance

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.JobCmdConfig
import za.co.absa.enceladus.conformance.{ConformanceCmdConfigT, ConformanceConfig}
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}
import za.co.absa.enceladus.standardization.{CobolOptions, StandardizationCmdConfigT, StandardizationConfig}

trait StdConformanceCmdConfigT[T] extends StandardizationCmdConfigT[T] with ConformanceCmdConfigT[T]

case class StdConformanceCmdConfig(datasetName: String = "",
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
                                   autocleanStandardizedFolder: Option[Boolean] = None
                                  ) extends StdConformanceCmdConfigT[StdConformanceCmdConfig]{

  override def withPublishPathOverride(value: Option[String]): StdConformanceCmdConfig = ???
  override def withExperimentalMappingRule(value: Option[Boolean]): StdConformanceCmdConfig = ???
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): StdConformanceCmdConfig = ???
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): StdConformanceCmdConfig = ???
  override def withDatasetName(value: String): StdConformanceCmdConfig = ???
  override def withDatasetVersion(value: Int): StdConformanceCmdConfig = ???
  override def withReportDate(value: String): StdConformanceCmdConfig = ???
  override def withReportVersion(value: Option[Int]): StdConformanceCmdConfig = ???
  override def withMenasCredentialsFactory(value: MenasCredentialsFactory): StdConformanceCmdConfig = ???
  override def withPerformanceMetricsFile(value: Option[String]): StdConformanceCmdConfig = ???
  override def withFolderPrefix(value: Option[String]): StdConformanceCmdConfig = ???
  override def withPersistStorageLevel(value: Option[StorageLevel]): StdConformanceCmdConfig = ???
  override def withRawFormat(value: String): StdConformanceCmdConfig = ???
  override def withCharset(value: Option[String]): StdConformanceCmdConfig = ???
  override def withRowTag(value: Option[String]): StdConformanceCmdConfig = ???
  override def withCsvDelimiter(value: Option[String]): StdConformanceCmdConfig = ???
  override def withCsvHeader(value: Option[Boolean]): StdConformanceCmdConfig = ???
  override def withCsvQuote(value: Option[String]): StdConformanceCmdConfig = ???
  override def withCsvEscape(value: Option[String]): StdConformanceCmdConfig = ???
  override def withCobolOptions(value: Option[CobolOptions]): StdConformanceCmdConfig = ???
  override def withFixedWidthTrimValues(value: Option[Boolean]): StdConformanceCmdConfig = ???
  override def withRawPathOverride(value: Option[String]): StdConformanceCmdConfig = ???
  override def withFailOnInputNotPerSchema(value: Boolean): StdConformanceCmdConfig = ???

  override def rawFormat: String = ???
  override def charset: Option[String] = ???
  override def rowTag: Option[String] = ???
  override def csvDelimiter: Option[String] = ???
  override def csvHeader: Option[Boolean] = ???
  override def csvQuote: Option[String] = ???
  override def csvEscape: Option[String] = ???
  override def cobolOptions: Option[CobolOptions] = ???
  override def fixedWidthTrimValues: Option[Boolean] = ???
  override def rawPathOverride: Option[String] = ???
  override def failOnInputNotPerSchema: Boolean = ???
}

object StdConformanceCmdConfigT {
  val stepName = "Conformance"

  def getCmdLineArguments(args: Array[String]): StdConformanceCmdConfigT[StdConformanceCmdConfig] = {
    //Mutually exclusive parameters don't work in scopt 4
    if (args.contains("--menas-credentials-file") && args.contains("--menas-auth-keytab")) {
      println("ERROR: Only one authentication method is allowed at a time")
      System.exit(1)
    }
    val optionCmd = OParser.parse(stdConfJobParser, args, StdConformanceCmdConfig())

    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  val stdConfJobParser: OParser[_, StdConformanceCmdConfig] = {
    val builder = OParser.builder[StdConformanceCmdConfig]
    import builder._
    OParser.sequence(
      programName("Standardization Conformance Job"),
      StandardizationConfig.standardizationParser,
      ConformanceConfig.conformanceParser,
      JobCmdConfig.jobConfigParser
    )
  }

}

