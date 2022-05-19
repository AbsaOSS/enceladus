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

  def setSystemProperty(key: String, value: String, overrideExisting: Boolean = false): Unit = {
    if (overrideExisting || System.getProperty(key) == null) {
      System.setProperty(key, value)
    }
  }

  /**
   * File path is check for existence on given path (returned as Some); as a fallback when not found on path,
   * file is attempted to be found in current directory instead (returned as Some(justFileName.ext) -
   * for spark-submit uses --files with aliases
   * None is returned otherwise if not found at all
   * @param pathFileName
   * @return defined existing file path or None
   */
  def getExistingFilePathWithCurrentDirFallback(pathFileName: String): Option[String] = {
    if (Files.exists(Paths.get(pathFileName))) {
      log.info(s"File exists: $pathFileName")
      Some(pathFileName)
    } else {
      log.info(s"File does not exist: $pathFileName")
      val fileNameInCurDir = Paths.get(pathFileName).getFileName
      if (Files.exists(fileNameInCurDir)) {
        log.info(s"File exists: ${fileNameInCurDir.toString} (in the current directory, not in $pathFileName)")
        Some(fileNameInCurDir.toString)
      } else {
        log.error(s"File does not exist: $pathFileName (nor ${fileNameInCurDir.toString} in the current directory)")
        None
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
