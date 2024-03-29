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

package za.co.absa.enceladus.conformance.interpreter.rules.mapping

import org.apache.spark.sql.functions._
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.NestedTestCaseFactory._
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory._
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.spark.commons.utils.JsonUtils
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements


class MappingRuleBroadcastSuite extends MappingInterpreterSuite {
  import spark.implicits._

  test("Test broadcasting mapping rule works exactly like the original mapping rule for a simple dataframe") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/simpleSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/simpleResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, true, true, simpleMappingRule)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"int_num", $"long_num", $"str_val", $"errCol", $"conformedIntNum")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting mapping rule works exactly like the original mapping rule for a simple dataframe and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/simpleMultiOutSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/simpleMultiOutResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, true, false, simpleMappingRuleMultipleOutputs)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"int_num", $"long_num", $"str_val", $"errCol", $"conformedIntNum" ,$"conformedNum", $"conformedBool")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting mapping rule works exactly like the original mapping rule when a default value is used") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/simpleSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/simpleDefValResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, true, true, simpleMappingRuleWithDefaultValue)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"int_num", $"long_num", $"str_val", $"errCol", $"conformedIntNum")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting mapping rule works exactly like the original mapping rule for a simple dataframe and multiple outputs with defaults") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/simpleMultiOutSchema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/simpleDefValMultiOutResults.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      simpleTestCaseFactory.getTestCase(true, true, false, simpleMappingRuleMultipleOutputsWithDefaults)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"int_num", $"long_num", $"str_val", $"errCol", $"conformedIntNum" ,$"conformedNum", $"conformedBool")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can output a struct column") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/nested1Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/nested1Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, nestedMappingRule1)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol", $"conformedNum1")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can output a struct column and another output") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/nested1SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/nested1ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, nestedMappingRule1Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1",
        $"array2", $"errCol", $"conformedNum1", $"conformedInt")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work for fields inside a struct") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/nested2Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/nested2Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, nestedMappingRule2)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol", $"conformedNum2")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work for struct fields at different levels") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/nested3Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/nested3Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, nestedMappingRule3)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"conformedNum3", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work for struct fields at different levels and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/nested3SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/nested3ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, nestedMappingRule3Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2",
        $"conformedNum3", $"conformedInt" , $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work on arrays") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array1Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array1Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule1)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array2", $"errCol", $"array1")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work on arrays and multi outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/array1SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/array1ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule1Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array2", $"errCol", $"array1")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work on arrays within arrays") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array2Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array2Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule2)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule can work on arrays within arrays and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/array2SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/array2ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule2Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule failure if key fields are in different array levels") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array3Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array3Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule3)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule failure if key fields are in different array levels and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/array3SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/array3ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule3Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when key fields are in different array levels for an array of array") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array4Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array4Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule4)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when key fields are in different array levels for an array of array and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/array4SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/array4ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule4Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when key fields are in different struct levels in a array of arrays") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array5Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array5Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule5)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when key fields are in different struct levels in a array of arrays and multiple outputs") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/multiple_output/array5SchemaMulti.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/multiple_output/array5ResultsMulti.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule5Multi)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when 3 key fields are at different array levels") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array6Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array6Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule6)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule when there are errors in the error column") {
    val expectedSchema = getResourceString("/interpreter/mappingCases/array7Schema.txt")
    val expectedResults = getResourceString("/interpreter/mappingCases/array7Results.json")

    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, arrayMappingRule2)

    val inputDf2 = inputDf.withColumn("errCol", array(typedLit(ErrorMessage("Initial", "000", "ErrMsg", "id", Seq(), Seq()))))

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf2)
      .select($"id", $"key1", $"key2", $"struct1", $"struct2", $"array1", $"array2", $"errCol")
      .cacheIfNotCachedYet()

    val actualSchema = cleanupContainsNullProperty(dfOut.schema.treeString)
    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test broadcasting rule failure if key fields are in different arrays") {
    val expectedResults = getResourceString("/interpreter/mappingCases/arrayWrongJoinResults.json")
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      nestedTestCaseFactory.getTestCase(true, true, true, wrongMappingRule1)

    val dfOut = DynamicInterpreter().interpret(dataset, inputDf)
      .select($"id", $"errCol")
      .cacheIfNotCachedYet()

    val actualResults = JsonUtils.prettySparkJSON( dfOut.orderBy("id").toJSON.collect())
    assertResults(actualResults, expectedResults)
  }
}
