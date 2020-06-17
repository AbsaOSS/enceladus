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

object SecureConfig {
  /**
   * Moves Kafka security configuration from the config to system properties
   * if it is not defined there already (regarding trustStore, keyStore and auth login config).
   *
   * This is needed to be executed at least once to initialize secure Kafka when running from Spark.
   *
   * @param conf A configuration.
   */
  def setSecureKafkaProperties(conf: Config): Unit = {
    setTrustStoreProperties(conf)
    setKeyStoreProperties(conf)
    ConfigUtils.setSystemPropertyFileFallback(conf, "java.security.auth.login.config")
  }

  /**
   * Sets `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword` system properties from the same-name values
   * in the `conf`
   * @param conf config to lookup values form
   */
  def setTrustStoreProperties(conf: Config): Unit = {
    ConfigUtils.setSystemPropertyFileFallback(conf, "javax.net.ssl.trustStore")
    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.ssl.trustStorePassword")
  }

  /**
   * Sets `javax.net.ssl.keyStore` and `javax.net.ssl.keyStorePassword` system properties from the same-name values
   * in the `conf`
   * @param conf config to lookup values form
   */
  def setKeyStoreProperties(conf: Config): Unit = {
    ConfigUtils.setSystemPropertyFileFallback(conf, "javax.net.ssl.keyStore")
    ConfigUtils.setSystemPropertyStringFallback(conf, "javax.net.ssl.keyStorePassword")
  }

}
