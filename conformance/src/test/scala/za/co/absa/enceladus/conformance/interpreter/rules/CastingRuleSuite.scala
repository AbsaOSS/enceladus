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

import org.apache.spark.sql.types._
import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.FunSuite
import org.slf4j.event.Level.ERROR
import za.co.absa.enceladus.conformance.cmd.ConformanceCmdConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches, RuleValidators}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.samples.CastingRuleSamples
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class CastingRuleSuite extends FunSuite with SparkTestBase with LoggerTestBase {
  private val ruleName = "Casting rule"
  private val columnName = "dummy"

  test("Casting conformance rule test") {

    import spark.implicits._
    val inputDf = spark.read.schema(CastingRuleSamples.ordersSchema).json(CastingRuleSamples.ordersData.toDS)

    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    implicit val progArgs: ConformanceCmdConfig = ConformanceCmdConfig(reportDate = "2017-11-01")
    val experimentalMR = true
    val isCatalystWorkaroundEnabled = true
    val enableCF: Boolean = false

    mockWhen(dao.getDataset("Orders Conformance", 1)) thenReturn CastingRuleSamples.ordersDS

    val mappingTablePattern = "{0}/{1}/{2}"

    import spark.implicits._
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)

    val conformed = DynamicInterpreter.interpret(CastingRuleSamples.ordersDS, inputDf).cache

    val conformedJSON = JsonUtils.prettySparkJSON(conformed.orderBy($"id").toJSON.collect)

    if (conformedJSON != CastingRuleSamples.conformedOrdersJSON) {
      logger.error("EXPECTED:")
      logger.error(CastingRuleSamples.conformedOrdersJSON)
      logger.error("ACTUAL:")
      logger.error(conformedJSON)
      logger.error("DETAILS (Input):")
      logDataFrameContent(inputDf, ERROR)
      logger.error("DETAILS (Conformed):")
      logDataFrameContent(conformed, ERROR)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }

  }

  test("Casting rule fields validation test") {
    val schema = CastingRuleSamples.ordersSchema
    val dsName = "dataset"

    // These fields should pass the validation
    RuleValidators.validateInputField(dsName, ruleName, schema, "id")
    RuleValidators.validateInputField(dsName, ruleName, schema, "date")
    RuleValidators.validateInputField(dsName, ruleName, schema, "items.qty")
    RuleValidators.validateFieldExistence(dsName, ruleName, schema, "id")
    RuleValidators.validateFieldExistence(dsName, ruleName, schema, "date")
    RuleValidators.validateFieldExistence(dsName, ruleName, schema, "items.qty")
    RuleValidators.validateOutputField(dsName, ruleName, schema, "conformedvalue")
    RuleValidators.validateOutputField(dsName, ruleName, schema, "items.value")
    RuleValidators.validateSameParent(dsName, ruleName, "order.item.id", "order.item.ty", "order.item.details")

    assert(intercept[ValidationException] {
      RuleValidators.validateInputField(dsName, ruleName, schema, "nosuchfield")
    }.getMessage contains "does not exist")

    assert(intercept[ValidationException] {
      RuleValidators.validateFieldExistence(dsName, ruleName, schema, "nosuchfield")
    }.getMessage contains "does not exist")

    assert(intercept[ValidationException] {
      RuleValidators.validateFieldExistence(dsName, ruleName, schema, "id", "nosuchfield")
    }.getMessage contains "does not exist")

    assert(intercept[ValidationException] {
      RuleValidators.validateInputField(dsName, ruleName, schema, "items")
    }.getMessage contains "is not a primitive")

    assert(intercept[ValidationException] {
      RuleValidators.validateOutputField(dsName, ruleName, schema, "id")
    }.getMessage contains "already exists so it cannot be used")

    assert(intercept[ValidationException] {
      RuleValidators.validateOutputField(dsName, ruleName, schema, "items")
    }.getMessage contains "already exists so it cannot be used")

    assert(intercept[ValidationException] {
      RuleValidators.validateOutputField(dsName, ruleName, schema, "items.qty")
    }.getMessage contains "already exists so it cannot be used")

    assert(intercept[ValidationException] {
      RuleValidators.validateOutputField(dsName, ruleName, schema, "id.conformed")
    }.getMessage contains "is a primitive type")

    assert(intercept[ValidationException] {
      RuleValidators.validateSameParent(dsName, ruleName, "id", "items.qty")
    }.getMessage contains "have different parents")

    assert(intercept[ValidationException] {
      RuleValidators.validateSameParent(dsName, ruleName, "order.item.id", "order.item.ty",
        "order.item.details.payment")
    }.getMessage contains "have different parents")

  }

  test("Test Casting rule ok handling conversion from 'long' to 'string'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, LongType, "string")
  }

  test("Test Casting rule ok handling conversion from 'string' to 'long'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, StringType, "long")
  }

  test("Test Casting rule ok handling conversion from 'string' to 'date'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, StringType, "date")
  }

  test("Test Casting rule ok handling conversion from 'boolean' to 'decimal(10,2)'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, BooleanType, "decimal(10,2)")
  }

  test("Test Casting rule ok handling conversion from 'long' to 'timestamp'") {
    // A long value is expected to hold the number of seconds since Epoch (1970-01-01T00:00:00.000+00:00) by default
    RuleValidators.validateTypeCompatibility(ruleName, columnName, LongType, "timestamp")
  }

  test("Test Casting rule ok handling conversion from 'timestamp' to 'boolean'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, TimestampType, "boolean")
  }

  test("Test Casting rule ok handling conversion from 'date' to 'boolean'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, DateType, "boolean")
  }

  test("Test Casting rule ok handling conversion from 'double' to 'timestamp'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, DoubleType, "timestamp")
  }

  test("Test Casting rule ok handling conversion from 'double' to 'long'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, DoubleType, "long")
  }

  test("Test Casting rule ok handling conversion from 'timestamp' to 'double'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, TimestampType, "double")
  }

  test("Test Casting rule ok handling conversion from 'date' to 'double'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, DateType, "double")
  }

  test("Test Casting rule ok handling conversion from 'long' to 'double'") {
    RuleValidators.validateTypeCompatibility(ruleName, columnName, LongType, "double")
  }

  test("Test Casting rule failure handling conversion from 'long' to 'date'") {
    val e = intercept[ValidationException] {
      RuleValidators.validateTypeCompatibility(ruleName, columnName, LongType, "date")
    }
    assert(e.message.contains("conversion from 'long' to 'date' is not supported"))
  }

  test("Test Casting rule failure handling conversion from 'boolean' to 'date'") {
    val e = intercept[ValidationException] {
      RuleValidators.validateTypeCompatibility(ruleName, columnName, BooleanType, "date")
    }
    assert(e.message.contains("conversion from 'boolean' to 'date' is not supported"))
  }

  test("Test Casting rule failure handling conversion from 'decimal(10,2)' to 'date'") {
    val e = intercept[ValidationException] {
      RuleValidators.validateTypeCompatibility(ruleName, columnName, DecimalType(10, 2), "date")
    }
    assert(e.message.contains("conversion from 'decimal(10,2)' to 'date' is not supported"))
  }

  test("Test Casting rule failure handling conversion from 'boolean' to 'timestamp'") {
    val e = intercept[ValidationException] {
      RuleValidators.validateTypeCompatibility(ruleName, columnName, BooleanType, "timestamp")
    }
    assert(e.message.contains("conversion from 'boolean' to 'timestamp' is not supported"))
  }

  test("Test Casting rule failure handling conversion from 'double' to 'date'") {
    val e = intercept[ValidationException] {
      RuleValidators.validateTypeCompatibility(ruleName, columnName, DoubleType, "date")
    }
    assert(e.message.contains("conversion from 'double' to 'date' is not supported"))
  }

}
