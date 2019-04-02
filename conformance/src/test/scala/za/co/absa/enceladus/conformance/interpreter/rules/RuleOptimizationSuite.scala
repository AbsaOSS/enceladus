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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.model.conformanceRule.{ArrayCollectPseudoRule, ArrayExplodePseudoRule, ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.samples.TradeConformance._

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

  test("Test non-mapping rules are not grouped") {
    val rules: List[ConformanceRule] = List(litRule, upperRule, lit2Rule)

    // The expected output rule list is just the original rule list with the order field recalculated
    val rulesExpected: List[ConformanceRule] = List(litRule.withUpdatedOrder(1),
      upperRule.withUpdatedOrder(2), lit2Rule.withUpdatedOrder(3))

    val optimizedRules: List[ConformanceRule] = DynamicInterpreter.getExplosionOptimizedSteps(rules, schema)

    assert(optimizedRules == rulesExpected)
  }

  test("Test mapping rules having the same array are grouped") {
    val rules: List[ConformanceRule] = List(litRule, countryRule, productRule, lit2Rule)

    val explodeRule = ArrayExplodePseudoRule(2, "legs.conditions", controlCheckpoint = false)
    val collectRule = ArrayCollectPseudoRule(5, "", controlCheckpoint = false)

    // The expected output rule list is just the original rule list with the order field recalculated
    val rulesExpected: List[ConformanceRule] = List(litRule.withUpdatedOrder(1), explodeRule,
      countryRule.withUpdatedOrder(3), productRule.withUpdatedOrder(4), collectRule,
      lit2Rule.withUpdatedOrder(6))

    val optimizedRules: List[ConformanceRule] = DynamicInterpreter.getExplosionOptimizedSteps(rules, schema)

    assert(optimizedRules == rulesExpected)
  }

  test("Test single arrays in the beginning and at the end") {
    val rules: List[ConformanceRule] = List(countryRule, litRule, lit2Rule, productRule)

    // The expected output rule list is just the original rule list with the order field recalculated
    val rulesExpected: List[ConformanceRule] = List(countryRule.withUpdatedOrder(1),
      litRule.withUpdatedOrder(2), lit2Rule.withUpdatedOrder(3),
      productRule.withUpdatedOrder(4))

    val optimizedRules: List[ConformanceRule] = DynamicInterpreter.getExplosionOptimizedSteps(rules, schema)

    assert(optimizedRules == rulesExpected)
  }

  test("Test several arrays in the beginning and at the end") {
    val rules: List[ConformanceRule] = List(countryRule, productRule, litRule, lit2Rule, productRule, countryRule)

    val explodeRule1 = ArrayExplodePseudoRule(1, "legs.conditions", controlCheckpoint = false)
    val collectRule1 = ArrayCollectPseudoRule(4, "", controlCheckpoint = false)
    val explodeRule2 = ArrayExplodePseudoRule(7, "legs.conditions", controlCheckpoint = false)
    val collectRule2 = ArrayCollectPseudoRule(10, "", controlCheckpoint = false)

    // The expected output rule list is just the original rule list with the order field recalculated
    val rulesExpected: List[ConformanceRule] = List(explodeRule1, countryRule.withUpdatedOrder(2),
      productRule.withUpdatedOrder(3), collectRule1, litRule.withUpdatedOrder(5),
      lit2Rule.withUpdatedOrder(6), explodeRule2, productRule.withUpdatedOrder(8),
      countryRule.withUpdatedOrder(9), collectRule2)

    val optimizedRules: List[ConformanceRule] = DynamicInterpreter.getExplosionOptimizedSteps(rules, schema)

    assert(optimizedRules == rulesExpected)
  }

  test("Test mapping different arrays in sequence") {

    val legIdRule = MappingConformanceRule(order = 1, mappingTable = "dummy", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("leg_code" -> "legs.legid"),
      targetAttribute = "leg_name", outputColumn = "legs.conformed_legid")

    val rules: List[ConformanceRule] = List(countryRule, productRule, legIdRule, countryRule, legIdRule,
      countryRule, productRule, legIdRule, legIdRule)

    val explodeRule1 = ArrayExplodePseudoRule(1, "legs.conditions", controlCheckpoint = false)
    val collectRule1 = ArrayCollectPseudoRule(4, "", controlCheckpoint = false)
    val explodeRule2 = ArrayExplodePseudoRule(8, "legs.conditions", controlCheckpoint = false)
    val collectRule2 = ArrayCollectPseudoRule(11, "", controlCheckpoint = false)
    val explodeRule3 = ArrayExplodePseudoRule(12, "legs", controlCheckpoint = false)
    val collectRule4 = ArrayCollectPseudoRule(15, "", controlCheckpoint = false)

    // The expected output rule list is just the original rule list with the order field recalculated
    val rulesExpected: List[ConformanceRule] = List(explodeRule1, countryRule.withUpdatedOrder(2),
      productRule.withUpdatedOrder(3), collectRule1, legIdRule.withUpdatedOrder(5),
      countryRule.withUpdatedOrder(6), legIdRule.withUpdatedOrder(7), explodeRule2,
      countryRule.withUpdatedOrder(9), productRule.withUpdatedOrder(10), collectRule2,
      explodeRule3, legIdRule.withUpdatedOrder(13), legIdRule.withUpdatedOrder(14),
      collectRule4)

    val optimizedRules: List[ConformanceRule] = DynamicInterpreter.getExplosionOptimizedSteps(rules, schema)

    assert(optimizedRules == rulesExpected)
  }

}
