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

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import za.co.absa.enceladus.utils.config.SecureConfig.StoreDef

class SecureConfigSuite extends AnyFlatSpec with Matchers {

  private val emptyConfig = ConfigFactory.empty()
  private val keyStoreNoPassConfig = emptyConfig.withAnyRefValue("javax.net.ssl.keyStore", "/path/to/keystore")
  private val keyStoreConfig = keyStoreNoPassConfig.withAnyRefValue("javax.net.ssl.keyStorePassword", "ksPwd1")

  private val trustStoreNoPassConfig = emptyConfig.withAnyRefValue("javax.net.ssl.trustStore", "/path/to/trustStore")
  private val trustStoreConfig = trustStoreNoPassConfig.withAnyRefValue("javax.net.ssl.trustStorePassword", "tsPwd1")

  "SecureConfig" should "load keyStoreProperties from config" in {
    SecureConfig.getKeyStoreProperties(emptyConfig) shouldBe None
    SecureConfig.getKeyStoreProperties(trustStoreConfig) shouldBe None

    SecureConfig.getKeyStoreProperties(keyStoreNoPassConfig) shouldBe Some(StoreDef("/path/to/keystore", None))
    SecureConfig.getKeyStoreProperties(keyStoreConfig) shouldBe Some(StoreDef("/path/to/keystore", Some("ksPwd1")))
  }

  it should "load trustStoreProperties from config" in {
    SecureConfig.getTrustStoreProperties(emptyConfig) shouldBe None
    SecureConfig.getTrustStoreProperties(keyStoreConfig) shouldBe None

    SecureConfig.getTrustStoreProperties(trustStoreNoPassConfig) shouldBe Some(StoreDef("/path/to/trustStore", None))
    SecureConfig.getTrustStoreProperties(trustStoreConfig) shouldBe Some(StoreDef("/path/to/trustStore", Some("tsPwd1")))
  }
}
