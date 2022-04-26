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

package za.co.absa.enceladus.conformance.interpreter

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.interpreter.fixtures.NestedStructsFixture
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, TZNormalizedSparkTestBase}

/**
  * The purpose of these tests is to ensure Catalyst optimizer issue is handled.
  *
  * Without applying a workaround any test in this suite makes Spark freeze.
  */
class NestedStructSuite extends AnyFunSuite with TZNormalizedSparkTestBase with NestedStructsFixture with HadoopFsTestBase {

  test("Test Dynamic Conformance does not hang on many mixed conformance rules") {
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(false)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    val conformed = DynamicInterpreter().interpret(nestedStructsDS, standardizedDf)

    assert(conformed.count() == 20)
  }

  test("Test Dynamic Conformance does not hang on many uppercase conformance rules") {
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(false)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    val conformed = DynamicInterpreter().interpret(nestedStructsUpperDS, standardizedDf)

    assert(conformed.count() == 20)
  }

  test("Test Dynamic Conformance does not hang on many negation conformance rules") {
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(false)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    val conformed = DynamicInterpreter().interpret( nestedStructsNegationDS, standardizedDf)

    assert(conformed.count() == 20)
  }

  test("Test Dynamic Conformance does not hang on many casting conformance rules") {
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(false)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    val conformed = DynamicInterpreter().interpret(nestedStructsCastingDS, standardizedDf)

    assert(conformed.count() == 20)
  }

}
