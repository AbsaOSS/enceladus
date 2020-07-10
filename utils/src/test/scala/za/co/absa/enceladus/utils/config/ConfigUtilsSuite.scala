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
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import scala.collection.JavaConverters._

class ConfigUtilsSuite extends FlatSpec with Matchers {

  val conf = ConfigFactory.parseMap(Map(
    "some.string.key" -> "string1",
    "some.boolean.key" -> true
    ).asJava
  )

  "ConfigUtils" should "correctly read optionString" in {
    conf.getOptionString("nonexistent.key") shouldBe None
    conf.getOptionString("some.string.key") shouldBe Some("string1")
  }

  it should "correctly read optionBoolean" in {
    conf.getOptionBoolean("nonexistent.key") shouldBe None
    conf.getOptionBoolean("some.boolean.key") shouldBe Some(true)
  }

}
