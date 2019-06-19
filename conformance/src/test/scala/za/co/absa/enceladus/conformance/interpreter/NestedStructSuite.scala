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

package za.co.absa.enceladus.conformance.interpreter

import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.interpreter.fixtures.NestedStructsFixture
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

/**
  * The purpose of these tests is to ensure Catalyst optimizer issue is handled.
  *
  * Without applying a workaround any test in this suite makes Spark freeze.
  */
class NestedStructSuite extends FunSuite with SparkTestBase with NestedStructsFixture{

  test("Test Dynamic Conformance does not hang on many mixed conformance rules"){
    val conformed = DynamicInterpreter.interpret(
      nestedStructsDS,
      standardizedDf,
      experimentalMappingRule = false,
      enableControlFramework = false
    )

    assert(conformed.count() == 20)
  }

  test("Test Dynamic Conformance does not hang on many uppercase conformance rules"){
    val conformed = DynamicInterpreter.interpret(
      nestedStructsUpperDS,
      standardizedDf,
      experimentalMappingRule = false,
      enableControlFramework = false
    )

    assert(conformed.count() == 20)
  }

  test("Test Dynamic Conformance does not hang on many negation conformance rules"){
    val conformed = DynamicInterpreter.interpret(
      nestedStructsNegationDS,
      standardizedDf,
      experimentalMappingRule = false,
      enableControlFramework = false
    )

    assert(conformed.count() == 20)
  }

}
