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

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import za.co.absa.enceladus.conformance.interpreter.{FeatureSwitches, ThreeStateSwitch}

class PropertiesProvider {
  private val enableCF: Boolean = true
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private implicit val conf: Config = ConfigFactory.load()

  def isAutocleanStdFolderEnabled()(implicit cmd: ConformanceConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.autocleanStandardizedFolder,
      "conformance.autoclean.standardized.hdfs.folder",
      defaultValue = false)
    log.info(s"Autoclean standardized HDFS folder = $enabled")
    enabled
  }

  def readFeatureSwitches()(implicit cmdConfig: ConformanceConfig): FeatureSwitches = FeatureSwitches()
    .setExperimentalMappingRuleEnabled(isExperimentalRuleEnabled())
    .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled())
    .setControlFrameworkEnabled(enableCF)
    .setBroadcastStrategyMode(broadcastingStrategyMode)
    .setBroadcastMaxSizeMb(broadcastingMaxSizeMb)

  private def isExperimentalRuleEnabled()(implicit cmd: ConformanceConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.experimentalMappingRule,
      "conformance.mapping.rule.experimental.implementation",
      defaultValue = false)
    log.info(s"Experimental mapping rule enabled = $enabled")
    enabled
  }

  private def isCatalystWorkaroundEnabled()(implicit cmd: ConformanceConfig): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.isCatalystWorkaroundEnabled,
      "conformance.catalyst.workaround",
      defaultValue = true)
    log.info(s"Catalyst workaround enabled = $enabled")
    enabled
  }

  private def broadcastingStrategyMode: ThreeStateSwitch = {
    ThreeStateSwitch(conf.getString("conformance.mapping.rule.broadcast"))
  }

  private def broadcastingMaxSizeMb: Int = {
    conf.getInt("conformance.mapping.rule.max.broadcast.size.mb")
  }

  /**
   * Returns an effective value of a parameter according to the following priorities:
   * - Command line arguments [highest]
   * - Configuration file (application.conf)
   * - Global default [lowest]
   *
   * @param cmdParameterOpt An optional value retrieved from command line arguments
   * @param configKey       A key in a configuration file
   * @param defaultValue    Global default value
   * @return The effective value of the parameter
   */
  private def getCmdOrConfigBoolean(cmdParameterOpt: Option[Boolean], configKey: String, defaultValue: Boolean)
                                   (implicit conf: Config): Boolean = {
    val enabled = cmdParameterOpt match {
      case Some(b) => b
      case None => conf.getOptionBoolean(configKey).getOrElse(defaultValue)
    }
    enabled
  }
}
