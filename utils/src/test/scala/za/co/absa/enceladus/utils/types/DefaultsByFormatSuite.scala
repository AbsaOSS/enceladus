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

package za.co.absa.enceladus.utils.types

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.config.ConfigReader

class DefaultsByFormatSuite extends AnyFunSuite {

  private val customTimestampConfig  = new ConfigReader(
    ConfigFactory.empty()
      .withValue("defaultTimestampTimeZone", ConfigValueFactory.fromAnyRef("UTC")) // fallback to "obsolete"
      .withValue("enceladus.defaultTimestampTimeZone.json", ConfigValueFactory.fromAnyRef("WrongTimeZone"))
  )

  test("Format specific timestamp time zone override exists") {
    val default = new DefaultsByFormat("xml")
    assert(default.getDefaultTimestampTimeZone.contains("Africa/Johannesburg"))
  }

  test("Format specific timestamp time zone override does not exists") {
    val default = new DefaultsByFormat("txt")
    assert(default.getDefaultTimestampTimeZone.contains("CET"))
  }

  test("Format specific timestamp zone fallbacks to obsolete") {
    val defaults = new DefaultsByFormat("xml", config = customTimestampConfig)
    assert(defaults.getDefaultTimestampTimeZone.contains("UTC"))
  }

  test("Format specific timestamp time zone override is not a valid time zone id") {
    intercept[IllegalStateException] {
      new DefaultsByFormat("json", config = customTimestampConfig)
    }
  }

  test("Date time zone does not exist at all") {
    val default = new DefaultsByFormat("testFormat")
    assert(default.getDefaultDateTimeZone.isEmpty)
  }

  private val customDateConfig = new ConfigReader(
    ConfigFactory.empty()
      .withValue("defaultDateTimeZone", ConfigValueFactory.fromAnyRef("UTC")) // fallback to "obsolete"
      .withValue("enceladus.defaultDateTimeZone.default", ConfigValueFactory.fromAnyRef("PST"))
      .withValue("enceladus.defaultDateTimeZone.csv", ConfigValueFactory.fromAnyRef("JST"))
      .withValue("enceladus.defaultDateTimeZone.parquet", ConfigValueFactory.fromAnyRef("Gibberish"))
  )

  test("Format specific date time zone override exists") {
    val defaults = new DefaultsByFormat("csv", config = customDateConfig)
    assert(defaults.getDefaultDateTimeZone.contains("JST"))
  }

  test("Format specific date time zone override does not exists") {
    val defaults = new DefaultsByFormat("testFormat", config = customDateConfig)
    assert(defaults.getDefaultDateTimeZone.contains("PST"))
  }

  test("Format specific date time zone override is not a valid time zone id") {
    intercept[IllegalStateException] {
      new DefaultsByFormat("parquet", config = customDateConfig)
    }
  }

  test("Getting the obsolete settings") {
    val localConfig = new ConfigReader(
      ConfigFactory.empty()
        .withValue("defaultTimestampTimeZone", ConfigValueFactory.fromAnyRef("PST"))
        .withValue("defaultDateTimeZone", ConfigValueFactory.fromAnyRef("JST"))
    )
    val defaults = new DefaultsByFormat("csv", config = localConfig)
    assert(defaults.getDefaultTimestampTimeZone.contains("PST"))
    assert(defaults.getDefaultDateTimeZone.contains("JST"))
  }

}
