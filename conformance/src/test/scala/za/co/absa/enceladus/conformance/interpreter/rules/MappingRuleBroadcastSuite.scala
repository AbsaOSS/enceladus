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

import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.NestedTestCaseFactory._
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory._
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.{NestedTestCaseFactory, SimpleTestCaseFactory}
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class MappingRuleBroadcastSuite extends FunSuite with SparkTestBase with LoggerTestBase with BeforeAndAfterAll {
  import spark.implicits._

  private val simpleTestCaseFactory = new SimpleTestCaseFactory()
  private val nestedTestCaseFactory = new NestedTestCaseFactory()

  override def beforeAll(): Unit = {
    super.beforeAll()
    simpleTestCaseFactory.createMappingTables()
    nestedTestCaseFactory.createMappingTables()
  }

  override def afterAll(): Unit = {
    simpleTestCaseFactory.deleteMappingTables()
    nestedTestCaseFactory.deleteMappingTables()
    super.afterAll()
  }

  test("Test broadcasting mapping rule works exactly like the original mapping rule for a simple dataframe") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/simpleSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/simpleResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, simpleMappingRule)

    val dfOut = DynamicInterpreter.interpret(dataset, inputDf).cache

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting mapping rule works exactly like the original mapping rule when a default value is used") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/simpleSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/simpleDefValResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, simpleMappingRuleWithDefaultValue)

    val dfOut = DynamicInterpreter.interpret(dataset, inputDf).cache

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can output a struct column") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/nested1Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/nested1Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, nestedMappingRule1)

    val dfOut = DynamicInterpreter.interpret(dataset, inputDf).cache

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work for fields in side a struct") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/nested2Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/nested2Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, nestedMappingRule2)

    val dfOut = DynamicInterpreter.interpret(dataset, inputDf).cache

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work on arrays") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array1Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array1Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, nestedMappingRule3)

    val dfOut = DynamicInterpreter.interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array2", $"errCol", $"array1")
      .cache

    val actualSchema = cleanupSchema(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  private def cleanupSchema(inputSchemaTree: String): String = {
    // This cleanup is needed since when a struct is processed via nestedStructMap() or nestedStructAndErrorMap(),
    // the new version of the struct always has the flag containsNull = false.
    inputSchemaTree
      .replaceAll("\\ \\(containsNull = true\\)", "")
      .replaceAll("\\ \\(containsNull = false\\)", "")
      .trim
  }

  private def getResourceString(name: String): String =
    IOUtils.toString(getClass.getResourceAsStream(name), "UTF-8")

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      logger.error("EXPECTED:")
      logger.error(expectedSchema)
      logger.error("ACTUAL:")
      logger.error(actualSchema)
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  private def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (!expectedResults.startsWith(actualResults)) {
      logger.error("EXPECTED:")
      logger.error(expectedResults)
      logger.error("ACTUAL:")
      logger.error(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }

}
