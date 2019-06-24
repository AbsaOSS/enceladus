/*
 * Copyright 2018-2019 ABSA Group Limited
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

import scala.util.matching.Regex

import org.apache.spark.sql.SparkSession

import scopt.OptionParser
import za.co.absa.enceladus.dao.menasplugin.MenasCredentials
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 *       Even if a field is mandatory it needs a default value.
 */
case class CmdConfig(datasetName: String = "",
    datasetVersion: Int = 1,
    reportDate: String = "",
    reportVersion: Option[Int] = None,
    menasCredentials: Option[Either[MenasCredentials, CmdConfig.KeytabLocation]] = None,
    performanceMetricsFile: Option[String] = None,
    publishPathOverride: Option[String] = None,
    folderPrefix: Option[String] = None,
    experimentalMappingRule: Option[Boolean] = None,
    isCatalystWorkaroundEnabled: Option[Boolean] = None)

object CmdConfig {

  type KeytabLocation = String

  def getCmdLineArguments(args: Array[String])(implicit spark: SparkSession): CmdConfig = {
    val parser = new CmdParser("spark-submit [spark options] ConformanceBundle.jar")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String)(implicit spark: SparkSession) extends OptionParser[CmdConfig](programName) {
    private val fsUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

    head("Dynamic Conformance", "")

    opt[String]('D', "dataset-name").required().action((value, config) =>
      config.copy(datasetName = value)).text("Dataset name")

    opt[Int]('d', "dataset-version").required().action((value, config) =>
      config.copy(datasetVersion = value)).text("Dataset version")
      .validate(value =>
        if (value > 0) success
        else failure("Option --dataset-version must be >0"))

    val reportDateMatcher: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r
    opt[String]('R', "report-date").required().action((value, config) =>
      config.copy(reportDate = value)).text("Report date in 'yyyy-MM-dd' format")
      .validate(value =>
        reportDateMatcher.findFirstIn(value) match {
          case None => failure(s"Match error in '$value'. Option --report-date expects a date in 'yyyy-MM-dd' format")
          case _    => success
        })

    opt[Int]('r', "report-version").optional().action((value, config) =>
      config.copy(reportVersion = Some(value)))
      .text("Report version. If not provided, it is inferred based on the publish path (it's an EXPERIMENTAL feature)")
      .validate(value =>
        if (value > 0) success
        else failure("Option --report-version must be >0"))

    private var credsFile: Option[String] = None
    private var keytabFile: Option[String] = None
    opt[String]("menas-credentials-file").hidden.optional().action({ (path, config) =>
      val credential = Left(MenasCredentials.fromFile(path))
      credsFile = Some(path)
      config.copy(menasCredentials = Some(credential))
    }).text("Path to Menas credentials config file.").validate(path =>
      if (keytabFile.isDefined) failure("Only one authentication method is allow at a time")
      else if (MenasCredentials.exists(MenasCredentials.replaceHome(path))) success
      else failure("Credentials file does not exist. Make sure you are running in client mode"))

    opt[String]("menas-auth-keytab").optional().action({ (file, config) =>
      keytabFile = Some(file)
      if(!fsUtils.localExists(file) && fsUtils.hdfsExists(file)) {
        config.copy(menasCredentials = Some(Right(fsUtils.hdfsFileToLocalTempFile(file))))
      } else {
      config.copy(menasCredentials = Some(Right(file)))
      }
    }).text("Path to keytab file used for authenticating to menas").validate({ file =>
      if (credsFile.isDefined) failure("Only one authentication method is allowed at a time")
      else if(fsUtils.exists(file)) success
      else failure("Keytab file doesn't exist")
    })

    opt[String]("performance-file").optional().action((value, config) =>
      config.copy(performanceMetricsFile = Option(value))).text("Produce a performance metrics file at the given location (local filesystem)")

    opt[String]("debug-set-publish-path").optional().hidden().action((value, config) =>
      config.copy(publishPathOverride = Option(value))).text("override the path of the published data (used internally for testing)")

    opt[String]("folder-prefix").optional().action((value, config) =>
      config.copy(folderPrefix = Option(value))).text("Adds a folder prefix before the infoDateColumn")

    opt[Boolean]("experimental-mapping-rule").optional().action((value, config) =>
      config.copy(experimentalMappingRule = Option(value))).text("Use experimental optimized mapping conformance rule")

    opt[Boolean]("catalyst-workaround").optional().action((value, config) =>
      config.copy(isCatalystWorkaroundEnabled = Option(value))).text("Turn on or off Catalyst workaround feature. " +
      "This overrides 'conformance.catalyst.workaround' configuration value provided in 'application.conf'.")

    help("help").text("prints this usage text")
  }

}
