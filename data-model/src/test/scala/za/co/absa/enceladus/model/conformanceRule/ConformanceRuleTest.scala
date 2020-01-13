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

package za.co.absa.enceladus.model.conformanceRule

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.{Matchers, WordSpec}

class ConformanceRuleTest extends WordSpec with Matchers {

  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  "CastingConformanceRule" should {
    val rule = CastingConformanceRule(order = 1, outputColumn = "conformed_col", controlCheckpoint = true, inputColumn = "id", outputDataType = "string")
    val json = """{"_t":"CastingConformanceRule","order":1,"outputColumn":"conformed_col","controlCheckpoint":true,"inputColumn":"id","outputDataType":"string"}"""
    assertSerDe(rule, json)
  }


  "ConcatenationConformanceRule" should {
    val rule = ConcatenationConformanceRule(order = 2, outputColumn = "conformed_col", controlCheckpoint = true, inputColumns = List("a.b.c", "drop"))
    val json = """{"_t":"ConcatenationConformanceRule","order":2,"outputColumn":"conformed_col","controlCheckpoint":true,"inputColumns":["a.b.c","drop"]}"""
    assertSerDe(rule, json)
  }


  "DropConformanceRule" should {
    val rule = DropConformanceRule(order = 3, controlCheckpoint = true, outputColumn = "drop")
    val json = """{"_t":"DropConformanceRule","order":3,"controlCheckpoint":true,"outputColumn":"drop"}"""
    assertSerDe(rule, json)
  }


  "LiteralConformanceRule" should {
    val rule = LiteralConformanceRule(order = 4, controlCheckpoint = true, outputColumn = "conformed_col", value = "lit")
    val json = """{"_t":"LiteralConformanceRule","order":4,"outputColumn":"conformed_col","controlCheckpoint":true,"value":"lit"}"""
    assertSerDe(rule, json)
  }

  "MappingConformanceRule" should {
    val rule = MappingConformanceRule(order = 5, controlCheckpoint = true, outputColumn = "conformed_country",
      mappingTable = "country", mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country"),
      targetAttribute = "country_name")
    val json = """{"_t":"MappingConformanceRule","order":5,"controlCheckpoint":true,"mappingTable":"country","mappingTableVersion":0,"attributeMappings":{"country_code":"country"},"targetAttribute":"country_name","outputColumn":"conformed_country","isNullSafe":false}"""
    assertSerDe(rule, json)
  }

  "NegationConformanceRule" should {
    val rule = NegationConformanceRule(order = 6, controlCheckpoint = true, outputColumn = "conformed_col", inputColumn = "asd")
    val json = """{"_t":"NegationConformanceRule","order":6,"outputColumn":"conformed_col","controlCheckpoint":true,"inputColumn":"asd"}"""
    assertSerDe(rule, json)
  }

  "SingleColumnConformanceRule" should {
    val rule = SingleColumnConformanceRule(order = 7, controlCheckpoint = true, outputColumn = "conformed_col", inputColumn = "input_col", inputColumnAlias = "input_alias")
    val json = """{"_t":"SingleColumnConformanceRule","order":7,"controlCheckpoint":true,"outputColumn":"conformed_col","inputColumn":"input_col","inputColumnAlias":"input_alias"}"""
    assertSerDe(rule, json)
  }


  "SparkSessionConfConformanceRule" should {
    val rule = SparkSessionConfConformanceRule(order = 8, controlCheckpoint = true, outputColumn = "asd", sparkConfKey = "spark.conf.key")
    val json = """{"_t":"SparkSessionConfConformanceRule","order":8,"outputColumn":"asd","controlCheckpoint":true,"sparkConfKey":"spark.conf.key"}"""
    assertSerDe(rule, json)
  }

  "UppercaseConformanceRule" should {
    val rule = UppercaseConformanceRule(order = 9, controlCheckpoint = true, outputColumn = "conformed_upper", inputColumn = "input_col")
    val json = """{"_t":"UppercaseConformanceRule","order":9,"outputColumn":"conformed_upper","controlCheckpoint":true,"inputColumn":"input_col"}"""
    assertSerDe(rule, json)
  }

  private def assertSerDe(rule: ConformanceRule, json: String) = {
    "serialize to a typed JSON" in {
      val serializedRule = objectMapper.writeValueAsString(rule)
      serializedRule shouldBe json
    }

    "deserialize polymorphically from a typed JSON" in {
      val deserializedRule = objectMapper.readValue(json, classOf[ConformanceRule])
      deserializedRule shouldBe rule
    }
  }

}
