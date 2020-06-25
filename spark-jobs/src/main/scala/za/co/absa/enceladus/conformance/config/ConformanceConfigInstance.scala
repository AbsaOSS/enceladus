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
import za.co.absa.enceladus.common.config.{ConfigError, JobConfig}
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory}

import scala.util.Try


/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 *       Even if a field is mandatory it needs a default value.
 */
case class ConformanceConfigInstance(datasetName: String = "",
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
                                     keytabFile: Option[String] = None
                                    )
  extends ConformanceConfig[ConformanceConfigInstance] with JobConfig[ConformanceConfigInstance] {

  override def withPublishPathOverride(value: Option[String]): ConformanceConfigInstance = copy(publishPathOverride = value)
  override def withExperimentalMappingRule(value: Option[Boolean]): ConformanceConfigInstance = copy(experimentalMappingRule = value)
  override def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): ConformanceConfigInstance =
    copy(isCatalystWorkaroundEnabled = value)
  override def withAutocleanStandardizedFolder(value: Option[Boolean]): ConformanceConfigInstance =
    copy(autocleanStandardizedFolder = value)
  override def withDatasetName(value: String): ConformanceConfigInstance = copy(datasetName = value)
  override def withDatasetVersion(value: Int): ConformanceConfigInstance = copy(datasetVersion = value)
  override def withReportDate(value: String): ConformanceConfigInstance = copy(reportDate = value)
  override def withReportVersion(value: Option[Int]): ConformanceConfigInstance = copy(reportVersion = value)
  override def withCredsFile(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): ConformanceConfigInstance = {
    copy(credsFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withAuthKeytab(value: Option[String], menasCredentialsFactory: MenasCredentialsFactory): ConformanceConfigInstance = {
    copy(keytabFile = value, menasCredentialsFactory = menasCredentialsFactory)
  }
  override def withPerformanceMetricsFile(value: Option[String]): ConformanceConfigInstance = copy(performanceMetricsFile = value)
  override def withFolderPrefix(value: Option[String]): ConformanceConfigInstance = copy(folderPrefix = value)
  override def withPersistStorageLevel(value: Option[StorageLevel]): ConformanceConfigInstance = copy(persistStorageLevel = value)
}

object ConformanceConfigInstance {
  def tryFromArguments(args: Array[String]): Try[ConformanceConfigInstance] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits._
    OParser.parse(conformanceJobParser, args, ConformanceConfigInstance()).toTry(ConfigError("Command line parameters error"))
  }

  def getFromArguments(args: Array[String]): ConformanceConfigInstance = tryFromArguments(args).get

  val conformanceJobParser: OParser[_, ConformanceConfigInstance] = {
    val builder = OParser.builder[ConformanceConfigInstance]
    import builder._
    OParser.sequence(
      programName("Conformance Job"),
      ConformanceConfig.conformanceParser,
      JobConfig.jobConfigParser
    )
  }

}
