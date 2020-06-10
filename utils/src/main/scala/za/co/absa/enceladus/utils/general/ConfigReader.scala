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

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object ConfigReader {
  private val config: Config = ConfigFactory.load()
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
   * Flattens TypeSafe config tree and returns the effective configuration
   * while redacting sensitive keys.
   *
   * @param sensitiveKeys A set of keys for which values shouldn't be logged.
   * @return the effective configuration as a map
   */
  def getFlatConfig(sensitiveKeys: Set[String] = Set()): Map[String, String] = {
    import collection.JavaConverters._

    val flatEntries = mutable.Map[String, String]()

    def redact(key: String, value: String): String = {
      if (sensitiveKeys.contains(key)) {
        "******"
      } else {
        value
      }
    }

    def render(path: String, obj: ConfigObject): Unit = {
      obj.asScala.foreach({ case (key, value) =>
        val flatKey = if (path.isEmpty) {
          key
        } else {
          s"$path.$key"
        }

        value match {
          case c: ConfigObject =>
            render(flatKey, c)
          case v: ConfigValue =>
            val redactedValue = redact(flatKey, v.render())
            flatEntries += flatKey -> redactedValue
        }
      })
    }

    render("", config.root())
    flatEntries.toMap
  }

  /**
   * Logs the effective configuration while redacting sensitive keys.
   *
   * @param sensitiveKeys A set of keys for which values shouldn't be logged.
   */
  def logEffectiveConfig(sensitiveKeys: Set[String] = Set()): Unit = {
    val flatConfig = getFlatConfig(sensitiveKeys)
    val renderedConfig = flatConfig
      .toSeq
      .sortBy(_._1)
      .map {
        case (k, v) => s"$k = $v"
      }
      .mkString("\n")

    log.info(s"Effective configuration:\n$renderedConfig")
  }
}
