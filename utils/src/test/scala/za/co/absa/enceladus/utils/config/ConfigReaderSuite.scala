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

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigReaderSuite extends AnyWordSpec with Matchers{
  private val config = ConfigFactory.parseString(
    """
      |top = default
      |quoted = "text"
      |redacted="12345"
      |nested {
      |  value.num = 100
      |  string = "str"
      |  redacted = "67890"
      |}
      |nothing=null
      |booleans {
      |  yes = "true"
      |  str = "xxx"
      |}
      |list = [1,2,3]
      |list2=["10", "20", "30"]
      |list3="1, 1, 2, 3 , 5"
      |list4=["0", "A", "BB"]
      |""".stripMargin)

  private val keysToRedact = Set("redacted", "nested.redacted", "redundant.key")

  private val configReader = ConfigReader(config)

  "hasPath" should {
    "return true if the key exists" in {
      assert(configReader.hasPath("nested.value.num"))
    }
    "return true if key exists even as parent" in {
      assert(configReader.hasPath("nested"))
    }
    "return false if the key exists" in {
      assert(!configReader.hasPath("does.not.exists"))
    }
  }

  "getString()" should {
    "return the value if the key exists" in {
      configReader.getString("nested.redacted") shouldBe "67890"
    }
    "throw if the key does not exist" in {
      intercept[ConfigException.Missing] {
        configReader.getString("redundant.key")
      }
    }
    "return a value converted to string if the value is not a string" in {
      configReader.getString("nested.value.num") shouldBe "100"
    }
    "throws if the value is null" in {
      intercept[ConfigException.Null] {
        configReader.getString("nothing")
      }
    }
  }

  "getInt()" should {
    "return the value if the key exists and" when {
      "is an integer" in {
        configReader.getInt("nested.value.num") shouldBe 100
      }
      "and the value cane be converted to int" in {
        configReader.getInt("nested.redacted") shouldBe 67890
      }
    }
    "throw if the key does not exist" in {
      intercept[ConfigException.Missing] {
        configReader.getInt("redundant.key")
      }
    }
    "throws if the value is not an integer" in {
      intercept[ConfigException.WrongType] {
        configReader.getInt("top")
      }
    }
  }

  "getBoolean()" should {
    "return the value if the key exists and is a boolean" in {
      assert(configReader.getBoolean("booleans.yes"))
    }
    "throw if the key does not exist" in {
      intercept[ConfigException.Missing] {
        configReader.getBoolean("booleans.not.exists")
      }
    }
    "throws if the value is not a boolean" in {
      intercept[ConfigException.WrongType] {
        configReader.getBoolean("booleans.str")
      }
    }
  }

  "getStringOption()" should {
    "return Some(value) if the key exists" in {
      assert(configReader.getStringOption("nested.redacted").contains("67890"))
    }
    "return None if the key does not exist" in {
      assert(configReader.getStringOption("redundant.key").isEmpty)
    }
    "return None if the key is Null" in {
      assert(configReader.getStringOption("nothing").isEmpty)
    }
  }
  
  "getFlatConfig()" should {
    "return the same config if there are no keys to redact" in {
      val redactedConfig = configReader.getFlatConfig(Set())

      redactedConfig("top") shouldBe "default"
      redactedConfig("quoted") shouldBe "text"
      redactedConfig("nested.value.num").toString shouldBe "100"
      redactedConfig("nested.string") shouldBe "str"
      redactedConfig("redacted") shouldBe "12345"
      redactedConfig("nested.redacted") shouldBe "67890"
      !redactedConfig.contains("redundant.key")
    }

    "redact an input config when given a set of keys to redact" in {
      val redactedConfig = configReader.getFlatConfig(keysToRedact)

      redactedConfig("top") shouldBe "default"
      redactedConfig("quoted") shouldBe "text"
      redactedConfig("nested.value.num").toString shouldBe "100"
      redactedConfig("nested.string") shouldBe "str"
      redactedConfig("redacted") shouldBe ConfigReader.redactedReplacement
      redactedConfig("nested.redacted") shouldBe ConfigReader.redactedReplacement
      assert(!redactedConfig.contains("redundant.key"))
    }
  }

  "getRedactedConfig()" should {
    "return the same config if there are no keys to redact" in {
      val redactedConfig = configReader.getRedactedConfig(Set())

      assertResult(config)(redactedConfig.config)
    }

    "redact an input config when given a set of keys to redact" in {
      val redactedConfig = configReader.getRedactedConfig(keysToRedact)

      redactedConfig.getString("top") shouldBe "default"
      redactedConfig.getString("quoted") shouldBe "text"
      redactedConfig.getInt("nested.value.num") shouldBe 100
      redactedConfig.getString("nested.string") shouldBe "str"
      redactedConfig.getString("redacted") shouldBe ConfigReader.redactedReplacement
      redactedConfig.getString("nested.redacted") shouldBe ConfigReader.redactedReplacement
      !redactedConfig.hasPath("redundant.key")
    }
  }

}
