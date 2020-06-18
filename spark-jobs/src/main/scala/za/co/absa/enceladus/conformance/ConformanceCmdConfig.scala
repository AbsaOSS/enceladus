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

package za.co.absa.enceladus.conformance

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.JobCmdConfig
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 *       Even if a field is mandatory it needs a default value.
 */
case class ConformanceCmdConfig(datasetName: String = "",
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
                                autocleanStandardizedFolder: Option[Boolean] = None)
  extends ConformanceConfig[ConformanceCmdConfig]
  with JobCmdConfig[ConformanceCmdConfig]{
  override def withPublishPathOverride(value: Option[String]): ConformanceCmdConfig = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): ConformanceCmdConfig = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): ConformanceCmdConfig = copy(isCatalystWorkaroundEnabled = value)
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): ConformanceCmdConfig = copy(autocleanStandardizedFolder = value)
  override def withDatasetName(value: String): ConformanceCmdConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): ConformanceCmdConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): ConformanceCmdConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): ConformanceCmdConfig = copy(reportVersion = value)
  override def withMenasCredentialsFactory(value: MenasCredentialsFactory): ConformanceCmdConfig = copy(menasCredentialsFactory = value)
  override def withPerformanceMetricsFile(value: Option[String]): ConformanceCmdConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): ConformanceCmdConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): ConformanceCmdConfig = copy(persistStorageLevel = value)
}

object ConformanceCmdConfig {
  val stepName = "Conformance"

  def getCmdLineArguments(args: Array[String]): ConformanceCmdConfig = {
    //Mutually exclusive parameters don't work in scopt 4
    if (args.contains("--menas-credentials-file") && args.contains("--menas-auth-keytab")) {
      println("ERROR: Only one authentication method is allowed at a time")
      System.exit(1)
    }
    val optionCmd = OParser.parse(conformanceJobParser, args, ConformanceCmdConfig())

    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  val conformanceJobParser: OParser[_, ConformanceCmdConfig] = {
    val builder = OParser.builder[ConformanceCmdConfig]
    import builder._
    OParser.sequence(
      programName("Conformance Job"),
      ConformanceConfig.conformanceParser,
      JobCmdConfig.jobConfigParser
    )
  }

}
