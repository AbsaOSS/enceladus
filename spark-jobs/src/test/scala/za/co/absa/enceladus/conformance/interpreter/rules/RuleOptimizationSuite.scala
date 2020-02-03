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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches, InterpreterContext, Never}
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.conformance.samples.TradeConformance._

class RuleOptimizationSuite extends FunSuite {

  private val schemaJson =
    """{
      |  "type" : "struct",
      |  "fields" : [ {
      |    "name" : "id",
      |    "type" : "long",
      |    "nullable" : true,
      |    "metadata" : { }
      |  }, {
      |    "name" : "legs",
      |    "type" : {
      |      "type" : "array",
      |      "elementType" : {
      |        "type" : "struct",
      |        "fields" : [ {
      |          "name" : "conditions",
      |          "type" : {
      |            "type" : "array",
      |            "elementType" : {
      |              "type" : "struct",
      |              "fields" : [ {
      |                "name" : "checks",
      |                "type" : {
      |                  "type" : "array",
      |                  "elementType" : {
      |                    "type" : "struct",
      |                    "fields" : [ {
      |                      "name" : "checkNums",
      |                      "type" : {
      |                        "type" : "array",
      |                        "elementType" : "string",
      |                        "containsNull" : true
      |                      },
      |                      "nullable" : true,
      |                      "metadata" : { }
      |                    } ]
      |                  },
      |                  "containsNull" : true
      |                },
      |                "nullable" : true,
      |                "metadata" : { }
      |              }, {
      |                "name" : "country",
      |                "type" : "string",
      |                "nullable" : true,
      |                "metadata" : { }
      |              }, {
      |                "name" : "currency",
      |                "type" : "string",
      |                "nullable" : true,
      |                "metadata" : { }
      |              }, {
      |                "name" : "product",
      |                "type" : "string",
      |                "nullable" : true,
      |                "metadata" : { }
      |              } ]
      |            },
      |            "containsNull" : true
      |          },
      |          "nullable" : true,
      |          "metadata" : { }
      |        }, {
      |          "name" : "legid",
      |          "type" : "long",
      |          "nullable" : true,
      |          "metadata" : { }
      |        } ]
      |      },
      |      "containsNull" : true
      |    },
      |    "nullable" : true,
      |    "metadata" : { }
      |  } ]
      |}
      |""".stripMargin
  private val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]

  private implicit val ictxDummy: InterpreterContext = InterpreterContext(schema,
    null,
    FeatureSwitches().setExperimentalMappingRuleEnabled(true).setBroadcastStrategyMode(Never),
    "dummy",
    null,
    null,
    null)

  test("Test non-mapping rules are not grouped") {
    val rules: List[ConformanceRule] = List(litRule, upperRule, lit2Rule)

    val actualInterpreters = DynamicInterpreter.getInterpreters(rules, schema)

    assert(actualInterpreters.length == 3)
    assert(actualInterpreters.head.isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(1).isInstanceOf[UppercaseRuleInterpreter])
    assert(actualInterpreters(2).isInstanceOf[LiteralRuleInterpreter])
  }

  test("Test mapping rules having the same array are grouped") {
    val rules: List[ConformanceRule] = List(litRule, countryRule, productRule, lit2Rule)

    val actualInterpreters = DynamicInterpreter.getInterpreters(rules, schema)

    assert(actualInterpreters.length == 6)
    assert(actualInterpreters.head.isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(1).isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(2).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(3).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(4).isInstanceOf[ArrayCollapseInterpreter])
    assert(actualInterpreters(5).isInstanceOf[LiteralRuleInterpreter])
  }

  test("Test single arrays in the beginning and at the end") {
    val rules: List[ConformanceRule] = List(countryRule, litRule, lit2Rule, productRule)

    val actualInterpreters = DynamicInterpreter.getInterpreters(rules, schema)

    assert(actualInterpreters.length == 4)
    assert(actualInterpreters.head.isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(1).isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(2).isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(3).isInstanceOf[MappingRuleInterpreterGroupExplode])
  }

  test("Test several arrays in the beginning and at the end") {
    val rules: List[ConformanceRule] = List(countryRule, productRule, litRule, lit2Rule, productRule, countryRule)

    val actualInterpreters = DynamicInterpreter.getInterpreters(rules, schema)

    assert(actualInterpreters.length == 10)
    assert(actualInterpreters.head.isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(1).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(2).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(3).isInstanceOf[ArrayCollapseInterpreter])
    assert(actualInterpreters(4).isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(5).isInstanceOf[LiteralRuleInterpreter])
    assert(actualInterpreters(6).isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(7).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(8).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(9).isInstanceOf[ArrayCollapseInterpreter])
  }

  test("Test mapping different arrays in sequence") {

    val legIdRule = MappingConformanceRule(order = 1, mappingTable = "dummy", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("leg_code" -> "legs.legid"),
      targetAttribute = "leg_name", outputColumn = "legs.conformed_legid")

    val rules: List[ConformanceRule] = List(countryRule, productRule, legIdRule, countryRule, legIdRule,
      countryRule, productRule, legIdRule, legIdRule)

    val actualInterpreters = DynamicInterpreter.getInterpreters(rules, schema)

    assert(actualInterpreters.length == 15)
    assert(actualInterpreters.head.isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(1).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(2).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(3).isInstanceOf[ArrayCollapseInterpreter])
    assert(actualInterpreters(4).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(5).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(6).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(7).isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(8).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(9).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(10).isInstanceOf[ArrayCollapseInterpreter])
    assert(actualInterpreters(11).isInstanceOf[ArrayExplodeInterpreter])
    assert(actualInterpreters(12).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(13).isInstanceOf[MappingRuleInterpreterGroupExplode])
    assert(actualInterpreters(14).isInstanceOf[ArrayCollapseInterpreter])
  }

}
