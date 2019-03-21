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

import org.apache.spark.sql.Dataset
import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.samples.NegationRuleSamples
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class NegationRuleSuite extends FunSuite with SparkTestBase {

  import spark.implicits._

  test("Negation rule field validation test") {
    val schema = NegationRuleSamples.schema

    assert(intercept[ValidationException] {
      NegationRuleInterpreter.validateInputField("dataset", schema, "no.such.field")
    }.getMessage contains "does not exist")

    assert(intercept[ValidationException] {
      NegationRuleInterpreter.validateInputField("dataset", schema, "date")
    }.getMessage contains "field is not a numeric type")
  }

  test("Negation conformance rule should negate positive numeric values") {
    val inputDataset = NegationRuleSamples.Positive.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Positive.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  test("Negation conformance rule should negate negative numeric values") {
    val inputDataset = NegationRuleSamples.Negative.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Negative.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  test("Negation conformance rule should not change zero numeric values (Keep in mind positive " +
    "and negative floating-point zero)") {
    val inputDataset = NegationRuleSamples.Zero.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Zero.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  test("Negation conformance rule should negate max numeric values") {
    val inputDataset = NegationRuleSamples.Max.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Max.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  test("Negation conformance rule should produce errors when negating min numeric values due to Silent " +
    "Overflow and set to default value without promoting the data type") {
    val inputDataset = NegationRuleSamples.Min.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Min.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  test("Negation conformance rule should disregard null numeric values") {
    val inputDataset = NegationRuleSamples.Null.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Null.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON)
  }

  private def testRule(inputDataset: Dataset[String], enceladusDataset: ConfDataset, expectedJSON: String): Unit = {
    val inputDf = spark.read.schema(NegationRuleSamples.schema).json(inputDataset)

    implicit val dao: EnceladusDAO = mock(classOf[EnceladusDAO])
    implicit val progArgs: CmdConfig = CmdConfig(reportDate = "2017-11-01")
    implicit val enableCF: Boolean = false
    mockWhen(dao.getDataset("Test Name", 1)) thenReturn enceladusDataset

    val conformed = DynamicInterpreter.interpret(enceladusDataset, inputDf, experimentalMappingRule = true).cache

    val conformedJSON = conformed.toJSON.collect().mkString("\n")

    if (conformedJSON != expectedJSON) {
      println("EXPECTED:")
      println(expectedJSON)
      println("ACTUAL:")
      println(conformedJSON)
      println("DETAILS (Input):")
      inputDf.printSchema()
      inputDf.show
      println("DETAILS (Conformed):")
      conformed.printSchema()
      conformed.show

      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }

}
