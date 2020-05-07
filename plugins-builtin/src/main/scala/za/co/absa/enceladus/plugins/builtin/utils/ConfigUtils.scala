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

package za.co.absa.enceladus.plugins.builtin.utils

import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.Config
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
   * Puts a file location from a configuration to the system properties ensusing the file exists.
   *
   * A file provided can be at an absolute path (for client mode), e.g. /home/aabb/file.conf,
   * but other locations are investigated as well in this order.
   *
   * - The exact path pased (/home/aabb/file.conf).
   * - The file in the current directory, when spark-submit uses --files with aliases (file.conf).
   * - The file in the distribution directoty
   *
   * @param conf  A configuration.
   * @param key   Configuration key.
   */
  def setSystemPropertyFileFallback(conf: Config, key: String): Unit = {
    if (System.getProperty(key) == null && conf.hasPath(key)) {
      val pathFileName = conf.getString(key)
      if (Files.exists(Paths.get(pathFileName))) {
        log.info(s"File exists: ${pathFileName.toString}")
        System.setProperty(key, conf.getString(key))
      } else {
        log.warn(s"File does not exist: ${pathFileName.toString}")
        val fileNameInCurDir = Paths.get(pathFileName).getFileName
        if (Files.exists(fileNameInCurDir)) {
          log.info(s"File exists: ${fileNameInCurDir.toString}")
          System.setProperty(key, fileNameInCurDir.toString)
        } else {
          log.warn(s"File does not exist: ${fileNameInCurDir.toString}")
          val sparkRoot = System.getProperty("spark.dist.files.location")
          if (sparkRoot == null) {
            log.warn(s"'spark.dist.files.location' is not set. Cannot relink filed to the distribution location in cluster mode.")
          } else {
            val fileName = Paths.get(pathFileName).getFileName.toString
            val newPath = Paths.get(sparkRoot, fileName).toAbsolutePath
            if (Files.exists(newPath)) {
              log.warn(s"File exists: ${newPath.toString}")
              System.setProperty(key, newPath.toString)
            } else {
              log.error(s"File does not exist: ${newPath.toString}")
              System.setProperty(key, newPath.toString)
            }
          }
        }
      }
    }
  }


}
