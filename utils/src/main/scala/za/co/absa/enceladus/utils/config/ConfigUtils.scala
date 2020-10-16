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

package za.co.absa.enceladus.utils.config

import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigValueFactory}
import org.slf4j.LoggerFactory

object ConfigUtils {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Puts a config key and value to the system properties if it is not defined there.
   *
   * @param conf  A configuration.
   * @param key   Configuration key.
   */
  def setSystemPropertyStringFallback(conf: Config, key: String): Unit = {
    if (System.getProperty(key) == null && conf.hasPath(key)) {
      System.setProperty(key, conf.getString(key))
    }
  }

  /**
   * Puts a file location from a configuration to the system properties ensuring the file exists.
   *
   * A file provided can be at an absolute path (for client mode), e.g. /home/aabb/file.conf,
   * but other locations are investigated as well in this order.
   *
   * - The exact path passed (/home/aabb/file.conf).
   * - The file in the current directory, when spark-submit uses --files with aliases (file.conf).
   *
   * @param conf  A configuration.
   * @param key   Configuration key.
   */
  def setSystemPropertyFileFallback(conf: Config, key: String): Unit = {
    if (System.getProperty(key) == null && conf.hasPath(key)) {
      val pathFileName = conf.getString(key)
      if (Files.exists(Paths.get(pathFileName))) {
        log.info(s"File exists: $pathFileName")
        System.setProperty(key, pathFileName)
      } else {
        log.info(s"File does not exist: $pathFileName")
        val fileNameInCurDir = Paths.get(pathFileName).getFileName
        if (Files.exists(fileNameInCurDir)) {
          log.info(s"File exists: ${fileNameInCurDir.toString} (in the current directory, not in $pathFileName)")
          System.setProperty(key, fileNameInCurDir.toString)
        } else {
          log.error(s"File does not exist: $pathFileName (nor ${fileNameInCurDir.toString} in the current directory)")
        }
      }
    }
  }

  implicit class ConfigImplicits(config: Config) {
    /**
     * Inspects the config for the presence of the `key` and returns an optional result.
     *
     * @param key path to look for, e.g. "group1.subgroup2.value3
     * @return None if not found or defined Option[String]
     */
    def getOptionString(key: String): Option[String] = {
      if (config.hasPath(key)) {
        Some(config.getString(key))
      } else {
        None
      }
    }

    /**
     * Inspects the config for the presence of the `key` and returns an optional result.
     *
     * @param key path to look for, e.g. "group1.subgroup2.value3
     * @return None if not found or defined Option[Boolean]
     */
    def getOptionBoolean(key: String): Option[Boolean] = {
      if (config.hasPath(key)) {
        Some(config.getBoolean(key))
      } else {
        None
      }
    }

    /** Handy shorthand of frequent `config.withValue(key, ConfigValueFactory.fromAnyRef(value))` */
    def withAnyRefValue(key: String, value: AnyRef) : Config = {
      config.withValue(key, ConfigValueFactory.fromAnyRef(value))
    }

  }

}
