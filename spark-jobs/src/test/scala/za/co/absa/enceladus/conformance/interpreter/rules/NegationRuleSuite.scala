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

import org.apache.spark.sql.Dataset
import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.event.Level.ERROR
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.conformance.samples.NegationRuleSamples
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.fs.HdfsUtils
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class NegationRuleSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase{

  import spark.implicits._

  test("Negation rule field validation test") {
    val schema = NegationRuleSamples.schema

    assert(intercept[ValidationException] {
      NegationRuleInterpreter.validateInputField("dataset", schema, "no.such.field")
    }.getMessage contains "does not exist")

    assert(intercept[ValidationException] {
      NegationRuleInterpreter.validateInputField("dataset", schema, "date")
    }.getMessage contains "field is neither NumericType nor BooleanType")
  }

  test("Negation conformance rule should negate positive numeric values") {
    val inputDataset = NegationRuleSamples.Positive.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Positive.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  test("Negation conformance rule should negate negative numeric values") {
    val inputDataset = NegationRuleSamples.Negative.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Negative.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  test("Negation conformance rule should not change zero numeric values (Keep in mind positive " +
    "and negative floating-point zero)") {
    val inputDataset = NegationRuleSamples.Zero.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Zero.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  test("Negation conformance rule should negate max numeric values") {
    val inputDataset = NegationRuleSamples.Max.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Max.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  test("Negation conformance rule should produce errors when negating min numeric values due to Silent " +
    "Overflow and set to default value without promoting the data type") {
    val inputDataset = NegationRuleSamples.Min.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Min.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = false)
  }

  test("Negation conformance rule should produce errors when negating min numeric values due to Silent " +
    "Overflow and set to null for nullable columns") {
    val inputDataset = NegationRuleSamples.MinWithNullableColumns.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.MinWithNullableColumns.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  test("Negation conformance rule should disregard null numeric values") {
    val inputDataset = NegationRuleSamples.Null.data.toDS
    val expectedDataset = NegationRuleSamples.dataset
    val expectedJSON = NegationRuleSamples.Null.conformedJSON

    testRule(inputDataset, expectedDataset, expectedJSON, nullableSchema = true)
  }

  private def testRule(inputDataset: Dataset[String], enceladusDataset: ConfDataset, expectedJSON: String, nullableSchema: Boolean): Unit = {
    val schema = if (nullableSchema) {
      NegationRuleSamples.schemaNullable
    } else {
      NegationRuleSamples.schema
    }
    val inputDf = spark.read.schema(schema).json(inputDataset)

    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    implicit val progArgs: ConformanceConfig = ConformanceConfig(reportDate = "2017-11-01")
    val experimentalMR = true
    val isCatalystWorkaroundEnabled = true
    val enableCF: Boolean = false
    mockWhen(dao.getDataset("Test Name", 1)) thenReturn enceladusDataset
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
    implicit val fsUtils: HdfsUtils = new HdfsUtils(spark.sparkContext.hadoopConfiguration)
    val conformed = DynamicInterpreter().interpret(enceladusDataset, inputDf).cache
    val conformedJSON = conformed.toJSON.collect().mkString("\n")
    if (conformedJSON != expectedJSON) {
      logger.error("EXPECTED:")
      logger.error(expectedJSON)
      logger.error("ACTUAL:")
      logger.error(conformedJSON)
      logger.error("DETAILS (Input):")
      logDataFrameContent(inputDf, ERROR)
      logger.error("DETAILS (Conformed):")
      logDataFrameContent(conformed, ERROR)

      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }

}
