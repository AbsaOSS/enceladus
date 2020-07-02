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

package za.co.absa.enceladus.conformance.config

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.common.config.{ConfigError, JobConfigParser}
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}

import scala.util.Try


/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 *       Even if a field is mandatory it needs a default value.
 */
case class ConformanceConfig(datasetName: String = "",
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
                             credsFile: Option[String] = None,
                             keytabFile: Option[String] = None)
  extends ConformanceParser[ConformanceConfig] {

  override def withPublishPathOverride(value: Option[String]): ConformanceConfig = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): ConformanceConfig = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): ConformanceConfig =
    copy(isCatalystWorkaroundEnabled = value)
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): ConformanceConfig =
    copy(autocleanStandardizedFolder = value)
  override def withDatasetName(value: String): ConformanceConfig = copy(datasetName = value)
  override def withDatasetVersion(value: Int): ConformanceConfig = copy(datasetVersion = value)
  override def withReportDate(value: String): ConformanceConfig = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): ConformanceConfig = copy(reportVersion = value)
  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): ConformanceConfig =
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)

  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): ConformanceConfig =
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)

  override def withPerformanceMetricsFile(value: Option[String]): ConformanceConfig = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): ConformanceConfig = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): ConformanceConfig = copy(persistStorageLevel = value)
}

object ConformanceConfig {
  def tryFromArguments(args: Array[String]): Try[ConformanceConfig] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits._
    OParser.parse(conformanceJobParser, args, ConformanceConfig()).toTry(ConfigError("Command line parameters error"))
  }

  def getFromArguments(args: Array[String]): ConformanceConfig = tryFromArguments(args).get

  val conformanceJobParser: OParser[_, ConformanceConfig] = {
    val builder = OParser.builder[ConformanceConfig]
    import builder._
    OParser.sequence(
      programName("Conformance Job"),
      ConformanceParser.conformanceParser,
      JobConfigParser.jobConfigParser
    )
  }

}
