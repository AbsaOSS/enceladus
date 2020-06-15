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

package za.co.absa.enceladus.utils.general

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.slf4j.LoggerFactory

object ConfigReader {
  val redactedReplacement: String = "*****"
}

class ConfigReader(config: Config = ConfigFactory.load()) {
  import ConfigReader._

  private val log = LoggerFactory.getLogger(this.getClass)

  def readStringConfigIfExist(path: String): Option[String] = {
    if (config.hasPath(path)) {
      Option(config.getString(path))
    } else {
      None
    }
  }

  def readStringConfig(path: String, default: String): String = {
    readStringConfigIfExist(path).getOrElse(default)
  }

  /**
   * Given a configuration returns a new configuration which has all sensitive keys redacted.
   *
   * @param keysToRedact  A set of keys to be redacted.
   */
  def getRedactedConfig(keysToRedact: Set[String]): Config = {
    def withAddedKey()(accumulatedConfig: Config, key: String): Config = {
      if (config.hasPath(key)) {
        accumulatedConfig.withValue(key, ConfigValueFactory.fromAnyRef(redactedReplacement))
      } else {
        accumulatedConfig
      }
    }

    val redactingConfig = keysToRedact.foldLeft(ConfigFactory.empty)(withAddedKey())

    redactingConfig.withFallback(config)
  }

  /**
   * Logs the effective configuration while redacting sensitive keys.
   *
   * @param keysToRedact A set of keys for which values shouldn't be logged.
   */
  def logEffectiveConfig(keysToRedact: Set[String] = Set()): Unit = {
    val redactedConfig = getRedactedConfig(keysToRedact)

    val renderOptions = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)

    val rendered = redactedConfig.root().render(renderOptions)

    log.info(s"Effective configuration:\n$rendered")
  }
}
