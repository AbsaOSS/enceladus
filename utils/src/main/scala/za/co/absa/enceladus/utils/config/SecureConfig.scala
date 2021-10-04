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

object SecureConfig {
  object Keys {
    val javaSecurityAuthLoginConfig = "java.security.auth.login.config"
    val javaxNetSslTrustStore = "javax.net.ssl.trustStore"
    val javaxNetSslTrustStorePassword = "javax.net.ssl.trustStorePassword"
    val javaxNetSslKeyStore = "javax.net.ssl.keyStore"
    val javaxNetSslKeyStorePassword = "javax.net.ssl.keyStorePassword"
  }

  case class StoreDef(path: String, password: Option[String])

  /**
   * Moves Kafka security configuration from the config to system properties
   * if it is not defined there already (regarding trustStore, keyStore and auth login config).
   *
   * This is needed to be executed at least once to initialize secure Kafka when running from Spark.
   *
   * @param conf A configuration.
   */
  def setSecureKafkaProperties(conf: ConfigReader): Unit = {
    setTrustStoreProperties(conf)
    setKeyStoreProperties(conf)
    ConfigUtils.setSystemPropertyFileFallback(conf, Keys.javaSecurityAuthLoginConfig)
  }

  /**
   * Sets `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword` system properties from the same-name values
   * in the `conf`
   * @param conf config to lookup values form
   */
  def setTrustStoreProperties(conf: ConfigReader): Unit = {
    ConfigUtils.setSystemPropertyFileFallback(conf, Keys.javaxNetSslTrustStore)
    ConfigUtils.setSystemPropertyStringFallback(conf, Keys.javaxNetSslTrustStorePassword)
  }

  def hasTrustStoreProperties(conf: ConfigReader): Boolean = {
    conf.hasPath(Keys.javaxNetSslTrustStore) &&
      conf.hasPath(Keys.javaxNetSslTrustStorePassword)
  }

  /**
   * Sets `javax.net.ssl.keyStore` and `javax.net.ssl.keyStorePassword` system properties from the same-name values
   * in the `conf`
   * @param conf config to lookup values form
   */
  def setKeyStoreProperties(conf: ConfigReader): Unit = {
    ConfigUtils.setSystemPropertyFileFallback(conf, Keys.javaxNetSslKeyStore)
    ConfigUtils.setSystemPropertyStringFallback(conf, Keys.javaxNetSslKeyStorePassword)
  }

  def hasKeyStoreProperties(conf: ConfigReader): Boolean = {
    conf.hasPath(Keys.javaxNetSslKeyStore) &&
      conf.hasPath(Keys.javaxNetSslKeyStorePassword)
  }

  /** Common logic to retrieve any store path with its optional password */
  private def getStoreProperties(conf: ConfigReader, storePathKey: String, storePasswordKey: String): Option[StoreDef] = {
    conf.getStringOption(storePathKey).map { storePath: String =>
      val optPassword = conf.getStringOption(storePasswordKey)

      StoreDef(storePath, optPassword)
    }
  }

  /**
   * Will yield optional KeyStore definition from `conf` from `Keys.javaxNetSslKeyStore` and
   * `Keys.javaxNetSslKeyStorePassword` (password is optional)
   * @param conf config
   * @return Defined KeyStore definition if `Keys.javaxNetSslKeyStore` exists
   */
  def getKeyStoreProperties(conf: ConfigReader): Option[StoreDef] =
    getStoreProperties(conf, Keys.javaxNetSslKeyStore, Keys.javaxNetSslKeyStorePassword)

  /**
   * Will yield optional TrustStore definition from `conf` from `Keys.javaxNetSslTrustStore` and
   * `Keys.javaxNetSslTrustStorePassword` (password is optional)
   * @param conf config
   * @return Defined TrustStore definition if `Keys.javaxNetSslKeyStore` exists
   */
  def getTrustStoreProperties(conf: ConfigReader): Option[StoreDef] =
    getStoreProperties(conf, Keys.javaxNetSslTrustStore, Keys.javaxNetSslTrustStorePassword)

}
