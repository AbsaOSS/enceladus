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

import scopt.OptionParser
import za.co.absa.enceladus.conformance.ConfCmdConfig.stepName

case class ConfConfig(publishPathOverride: Option[String] = None,
                      experimentalMappingRule: Option[Boolean] = None,
                      isCatalystWorkaroundEnabled: Option[Boolean] = None,
                      autocleanStandardizedFolder: Option[Boolean] = None)

object ConfConfig {

  def getCmdLineArguments(args: Array[String]): ConfConfig = {
    val parser = new CmdParser(s"spark-submit [spark options] ${stepName}Bundle.jar")

    val optionCmd = parser.parse(args, ConfConfig())
    optionCmd.getOrElse(ConfConfig())
  }

  private class CmdParser(programName: String) extends OptionParser[ConfConfig](programName) {
    override def errorOnUnknownArgument: Boolean = false
    override def reportWarning(msg: String): Unit = {}
    head("Dynamic Conformance", "")

    opt[String]("debug-set-publish-path").optional().hidden().action((value, config) =>
      config.copy(publishPathOverride = Option(value))).text("override the path of the published data (used internally for testing)")

    opt[Boolean]("experimental-mapping-rule").optional().action((value, config) =>
      config.copy(experimentalMappingRule = Option(value))).text("Use experimental optimized mapping conformance rule")

    opt[Boolean]("catalyst-workaround").optional().action((value, config) =>
      config.copy(isCatalystWorkaroundEnabled = Option(value))).text("Turn on or off Catalyst workaround feature. " +
      "This overrides 'conformance.catalyst.workaround' configuration value provided in 'application.conf'.")

    opt[Boolean]("autoclean-std-folder").optional().action((value, config) =>
      config.copy(autocleanStandardizedFolder = Option(value))).text("Deletes standardized data from HDFS once " +
      "it is successfully conformed. This overrides 'conformance.autoclean.standardized.hdfs.folder' configuration " +
      " value provided in 'application.conf'.")

    help("help").text("prints this usage text")
  }
}

