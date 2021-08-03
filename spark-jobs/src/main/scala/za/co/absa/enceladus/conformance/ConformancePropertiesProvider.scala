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
import za.co.absa.enceladus.conformance.config.ConformanceConfigParser
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import za.co.absa.enceladus.conformance.interpreter.{FeatureSwitches, ThreeStateSwitch}
import ConformancePropertiesProvider._
import za.co.absa.enceladus.common.ErrorColNormalization

/**
 * Reads conformance properties from the configuration file
 */
class ConformancePropertiesProvider {
  private val enableCF: Boolean = true
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private implicit val conf: Config = ConfigFactory.load()

  def isAutocleanStdFolderEnabled[T]()(implicit cmd: ConformanceConfigParser[T]): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.autocleanStandardizedFolder, standardizedHdfsFolderKey, defaultValue = false)
    log.info(s"Autoclean standardized HDFS folder = $enabled")
    enabled
  }

  def readFeatureSwitches[T]()(implicit cmdConfig: ConformanceConfigParser[T]): FeatureSwitches = {
    FeatureSwitches()
      .setExperimentalMappingRuleEnabled(isExperimentalRuleEnabled())
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled())
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(broadcastingStrategyMode)
      .setBroadcastMaxSizeMb(broadcastingMaxSizeMb)
      .setOriginalColumnsMutability(isOriginalColumnsMutabilityEnabled)
      .setErrColNullability(isErrColNullability)
  }

  private def isExperimentalRuleEnabled[T]()(implicit cmd: ConformanceConfigParser[T]): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.experimentalMappingRule, experimentalRuleKey, defaultValue = false)
    log.info(s"Experimental mapping rule enabled = $enabled")
    enabled
  }

  private def isCatalystWorkaroundEnabled[T]()(implicit cmd: ConformanceConfigParser[T]): Boolean = {
    val enabled = getCmdOrConfigBoolean(cmd.isCatalystWorkaroundEnabled, catalystWorkaroundKey, defaultValue = true)
    log.info(s"Catalyst workaround enabled = $enabled")
    enabled
  }

  private def isOriginalColumnsMutabilityEnabled[T]: Boolean = {
    val enabled = conf.getOptionBoolean(allowOriginalColumnsMutabilityKey).getOrElse(false)
    log.info(s"Original column mutability enabled = $enabled")
    enabled
  }

  private def isErrColNullability[T]: Boolean = {
    val nullability = ErrorColNormalization.getErrorColNullabilityFromConfig(conf)
    log.info(s"ErrCol nullability in Conformance = $nullability")
    nullability
  }

  private def broadcastingStrategyMode: ThreeStateSwitch = {
    ThreeStateSwitch(conf.getString(broadcastStrategyKey))
  }

  private def broadcastingMaxSizeMb: Int = {
    conf.getInt(maxBroadcastSizeKey)
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

object ConformancePropertiesProvider {
  private val standardizedHdfsFolderKey = "conformance.autoclean.standardized.hdfs.folder"
  private val maxBroadcastSizeKey = "conformance.mapping.rule.max.broadcast.size.mb"
  private val experimentalRuleKey = "conformance.mapping.rule.experimental.implementation"
  private val catalystWorkaroundKey = "conformance.catalyst.workaround"
  private val broadcastStrategyKey = "conformance.mapping.rule.broadcast"
  private val allowOriginalColumnsMutabilityKey = "conformance.allowOriginalColumnsMutability"
}
