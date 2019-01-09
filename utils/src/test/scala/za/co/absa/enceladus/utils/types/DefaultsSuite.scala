/*
 * Copyright 2018-2019 ABSA Group Limited
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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types.{DateType, Metadata, StructField, TimestampType}
import org.scalatest.FunSuite

class DefaultsSuite extends FunSuite {

  test("Timestamp with default value and pattern - should parse the given default based on the pattern and return a Timestamp with it.") {
    val timestamp = Defaults.getDefaultValue(StructField("timestamp", TimestampType, metadata = Metadata.fromJson("""{"default": "20250101.142626", "pattern": "yyyyMMdd.HHmmss"}""")))
    assert(timestamp == new Timestamp(1735741586000L))
  }

  test("Timestamp with default value, no pattern - should parse the given default based on the default pattern and return a Timestamp with it.") {
    val timestamp = Defaults.getDefaultValue(StructField("timestamp", TimestampType, metadata = Metadata.fromJson("""{"default": "2025-01-01 14:26:26"}""")))
    assert(timestamp == new Timestamp(1735741586000L))
  }

  test("Timestamp with pattern, no default value - should return the global default Timestamp.") {
    val timestamp = Defaults.getDefaultValue(StructField("timestamp", TimestampType, metadata = Metadata.fromJson("""{"pattern": "yyyyMMdd.HHmmss"}""")))
    assert(timestamp == new Timestamp(0L))
  }

  test("Timestamp without default value or pattern - should return the global default Timestamp.") {
    val timestamp = Defaults.getDefaultValue(StructField("timestamp", TimestampType, metadata = Metadata.fromJson("""{}""")))
    assert(timestamp == new Timestamp(0L))
  }

  test("Date with default value and pattern - should parse the given default based on the pattern and return a Date with it.") {
    val date = Defaults.getDefaultValue(StructField("date", DateType, metadata = Metadata.fromJson("""{"default": "20250101", "pattern": "yyyyMMdd"}""")))
    assert(date == new Date(1735689600000L))
  }

  test("Date with default value, no pattern - should parse the given default based on the default pattern and return a Date with it.") {
    val date = Defaults.getDefaultValue(StructField("date", DateType, metadata = Metadata.fromJson("""{"default": "2025-01-01"}""")))
    assert(date == new Date(1735689600000L))
  }

  test("Date with pattern, no default value - should return the global default Date.") {
    val date = Defaults.getDefaultValue(StructField("date", DateType, metadata = Metadata.fromJson("""{"pattern": "yyyyMMdd"}""")))
    assert(date == new Date(0L))
  }

  test("Date without default value or pattern - should return the global default Date.") {
    val date = Defaults.getDefaultValue(StructField("date", DateType, metadata = Metadata.fromJson("""{}""")))
    assert(date == new Date(0L))
  }

}
