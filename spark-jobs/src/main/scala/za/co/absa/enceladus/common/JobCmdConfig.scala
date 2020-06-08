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

package za.co.absa.enceladus.common

import org.apache.spark.storage.StorageLevel
import scopt.OptionParser
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentialsFactory, MenasCredentialsFactory, MenasKerberosCredentialsFactory, MenasPlainCredentialsFactory}

import scala.util.matching.Regex

case class JobCmdConfig(args: Array[String] = Array.empty,
                        datasetName: String = "",
                        datasetVersion: Int = 1,
                        reportDate: String = "",
                        reportVersion: Option[Int] = None,
                        menasCredentialsFactory: MenasCredentialsFactory = InvalidMenasCredentialsFactory,
                        performanceMetricsFile: Option[String] = None,
                        folderPrefix: Option[String] = None,
                        persistStorageLevel: Option[StorageLevel] = None)

object JobCmdConfig {
  def getCmdLineArguments(args: Array[String], step: String): JobCmdConfig = {
    val parser = new CmdParser(s"spark-submit [spark options] ${step}Bundle.jar")

    val optionCmd: Option[JobCmdConfig] = parser.parse(args, JobCmdConfig(args))

    optionCmd.getOrElse(JobCmdConfig())
  }

  private class CmdParser(programName: String) extends OptionParser[JobCmdConfig](programName) {
    override def errorOnUnknownArgument = false

    opt[String]('D', "dataset-name").required().action((value, config) =>
      config.copy(datasetName = value)).text("Dataset name")

    opt[Int]('d', "dataset-version").required().action((value, config) =>
      config.copy(datasetVersion = value)).text("Dataset version")
      .validate(value =>
        if (value > 0) {
          success
        } else {
          failure("Option --dataset-version must be >0")
        })
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
        if (value > 0) {
          success
        } else {
          failure("Option --report-version must be >0")
        })
    private var credsFile: Option[String] = None
    private var keytabFile: Option[String] = None
    opt[String]("menas-credentials-file").hidden.optional().action({ (file, config) =>
      credsFile = Some(file)
      config.copy(menasCredentialsFactory = new MenasPlainCredentialsFactory(file))
    }).text("Path to Menas credentials config file.").validate(path =>
      if (keytabFile.isDefined) {
        failure("Only one authentication method is allow at a time")
      } else {
        success
      })

    opt[String]("menas-auth-keytab").optional().action({ (file, config) =>
      keytabFile = Some(file)
      config.copy(menasCredentialsFactory = new MenasKerberosCredentialsFactory(file))
    }).text("Path to keytab file used for authenticating to menas").validate({ file =>
      if (credsFile.isDefined) {
        failure("Only one authentication method is allowed at a time")
      } else  {
        success
      }
    })

    opt[String]("performance-file").optional().action((value, config) =>
      config.copy(performanceMetricsFile = Option(value))).text("Produce a performance metrics file at the given location (local filesystem)")

    opt[String]("folder-prefix").optional().action((value, config) =>
      config.copy(folderPrefix = Option(value))).text("Adds a folder prefix before the infoDateColumn")

    opt[String]("persist-storage-level").optional().action((value, config) =>
      config.copy(persistStorageLevel = Some(StorageLevel.fromString(value))))
      .text("Specifies persistence storage level to use when processing data. Spark's default is MEMORY_AND_DISK.")

    checkConfig { config =>
      config.menasCredentialsFactory match {
        case InvalidMenasCredentialsFactory => failure("No authentication method specified (e.g. --menas-auth-keytab)")
        case _ => success
      }
    }
  }
}

final case class PathCfg(inputPath: String, outputPath: String)
