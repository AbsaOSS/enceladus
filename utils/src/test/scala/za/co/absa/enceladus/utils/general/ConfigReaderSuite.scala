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

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class ConfigReaderSuite extends WordSpec {
  private val config = ConfigFactory.parseString(
    """
      |top = default
      |quoted = "text"
      |password="12345"
      |nested {
      |  value.num = 100
      |  string = "str"
      |  password = "67890"
      |}
      |""".stripMargin)

  private val keysToRedact = Set("password", "nested.password", "redundant.key")

  private val configReader = new ConfigReader(config)

  "readStringConfigIfExist()" should {
    "return Some(value) if the key exists" in {
      assert(configReader.readStringConfigIfExist("nested.password").contains("67890"))
    }

    "return None if the key does not exist" in {
      assert(configReader.readStringConfigIfExist("redundant.key").isEmpty)
    }

    "return a value converted to string if the value is not a string" in {
      assert(configReader.readStringConfigIfExist("nested.value.num").contains("100"))
    }
  }

  "readStringConfig()" should {
    "return the value if the key exists" in {
      assert(configReader.readStringConfig("nested.password", "def") == "67890")
    }

    "return the default value if the key does not exist" in {
      assert(configReader.readStringConfig("redundant.key", "def") == "def")
    }

    "return a value converted to string if the value is not a string" in {
      assert(configReader.readStringConfig("nested.value.num", "def") == "100")
    }
  }

  "getRedactedConfig()" should {
    "return the same config if there are no keys to redact" in {
      val redactedConfig = configReader.getRedactedConfig(Set())

      assert(redactedConfig.getString("top") == "default")
      assert(redactedConfig.getString("quoted") == "text")
      assert(redactedConfig.getString("password") == "12345")
      assert(redactedConfig.getInt("nested.value.num") == 100)
      assert(redactedConfig.getString("nested.password") == "67890")
      assert(!redactedConfig.hasPath("redundant.key"))
    }

    "redact an input config when given a set of keys to redact" in {
      val redactedConfig = configReader.getRedactedConfig(keysToRedact)

      new ConfigReader(config).logEffectiveConfig(keysToRedact)

      assert(redactedConfig.getString("top") == "default")
      assert(redactedConfig.getString("quoted") == "text")
      assert(redactedConfig.getInt("nested.value.num") == 100)
      assert(redactedConfig.getString("nested.string") == "str")
      assert(redactedConfig.getString("password") == ConfigReader.redactedReplacement)
      assert(redactedConfig.getString("nested.password") == ConfigReader.redactedReplacement)
      assert(!redactedConfig.hasPath("redundant.key"))
    }

  }

}
