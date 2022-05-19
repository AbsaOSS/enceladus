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

import com.typesafe.config.Config
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits

object SecureConfig {
  object Keys {
    val javaSecurityAuthLoginConfig = "java.security.auth.login.config"
    val javaxNetSslTrustStore = "javax.net.ssl.trustStore"
    val javaxNetSslTrustStorePassword = "javax.net.ssl.trustStorePassword"
    val javaxNetSslKeyStore = "javax.net.ssl.keyStore"
    val javaxNetSslKeyStorePassword = "javax.net.ssl.keyStorePassword"
  }

  case class StoreDef(path: String, password: Option[String])

  def setSystemProperties(props: Map[String, String], overrideExisting: Boolean = false): Unit = {
    props.foreach { case (key, value) =>
      ConfigUtils.setSystemProperty(key, value, overrideExisting)
    }
  }

  /**
   * Reads SSL security configuration from the - trustStore, keyStore and auth login config
   * (e.g. can be used for Secure Kafka connections)
   *
   * @param conf
   * @return
   */
  def getSslProperties(conf: Config, useCurrentDirectoryPaths: Boolean = false): Map[String, String] = {

    // either use existing-checked files or directly stripped to current-directory files
    def getConfigFilePath: (Config, String) => Option[(String, String)] =
      if (useCurrentDirectoryPaths) getConfigKeyValueForStrippedFile else getConfigKeyValueForExistingFile

    Map.empty ++
      getConfigFilePath(conf, Keys.javaxNetSslTrustStore) ++ // all: Some(key -> value) or None
      getConfigKeyValue(conf, Keys.javaxNetSslTrustStorePassword) ++
      getConfigFilePath(conf, Keys.javaxNetSslKeyStore) ++
      getConfigKeyValue(conf, Keys.javaxNetSslKeyStorePassword) ++
      getConfigFilePath(conf, Keys.javaSecurityAuthLoginConfig)
  }

  private def getConfigKeyValue(conf: Config, key: String): Option[(String, String)] = {
    conf.getOptionString(key).map(key -> _)
  }

  /**
   * Path of the file is checked and only if found, the key-value pair is defined
   */
  private def getConfigKeyValueForExistingFile(conf: Config, key: String): Option[(String, String)] = {
    for {
      tsPath <- conf.getOptionString(key)
      tsExistingPath <- ConfigUtils.getExistingFilePathWithCurrentDirFallback(tsPath)
    } yield (key -> tsExistingPath)
  }

  /**
   * Path of the file is not checked -> the path is stripped for current-directory usage (e.g. with --files)
   */
  private def getConfigKeyValueForStrippedFile(conf: Config, key: String): Option[(String, String)] = {
    for {
      tsPath <- conf.getOptionString(key)
      strippedPath = tsPath.split('/').last
    } yield (key -> strippedPath)
  }

  /** Common logic to retrieve any store path with its optional password */
  private def getStoreProperties(conf: Config, storePathKey: String, storePasswordKey: String): Option[StoreDef] = {
    conf.getOptionString(storePathKey).map { storePath: String =>
      val optPassword = conf.getOptionString(storePasswordKey)

      StoreDef(storePath, optPassword)
    }
  }

  /**
   * Will yield optional KeyStore definition from `conf` from `Keys.javaxNetSslKeyStore` and
   * `Keys.javaxNetSslKeyStorePassword` (password is optional)
   *
   * @param conf config
   * @return Defined KeyStore definition if `Keys.javaxNetSslKeyStore` exists
   */
  def getKeyStoreProperties(conf: Config): Option[StoreDef] =
    getStoreProperties(conf, Keys.javaxNetSslKeyStore, Keys.javaxNetSslKeyStorePassword)

  /**
   * Will yield optional TrustStore definition from `conf` from `Keys.javaxNetSslTrustStore` and
   * `Keys.javaxNetSslTrustStorePassword` (password is optional)
   *
   * @param conf config
   * @return Defined TrustStore definition if `Keys.javaxNetSslKeyStore` exists
   */
  def getTrustStoreProperties(conf: Config): Option[StoreDef] =
    getStoreProperties(conf, Keys.javaxNetSslTrustStore, Keys.javaxNetSslTrustStorePassword)

  /**
   * will create java opts string e.g. "-Dkey.one=value.one -Dkey.two=value.two"
   *
   * @param configMap
   * @return
   */
  def javaOptsStringFromConfigMap(configMap: Map[String, String]): String = {
    configMap
      .map { case (key, value) => s"-D$key=$value" } // java opts looks like this -Dval.name=val.value
      .mkString(" ")
  }

}
