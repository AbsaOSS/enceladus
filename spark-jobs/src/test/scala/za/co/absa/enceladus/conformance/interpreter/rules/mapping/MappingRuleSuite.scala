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

import org.apache.spark.sql.AnalysisException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory._
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, LoggerTestBase, TZNormalizedSparkTestBase}
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

class MappingRuleSuite extends AnyFunSuite with TZNormalizedSparkTestBase with LoggerTestBase with BeforeAndAfterAll with HadoopFsTestBase {
  private val testCaseFactory = new SimpleTestCaseFactory()

  override def beforeAll(): Unit = {
    super.beforeAll()
    testCaseFactory.createMappingTables()
  }

  test("Test non-existent mapping table directory handling in a mapping rule") {
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      testCaseFactory.getTestCase(true, false, false, nonExistentTableMappingRule)

    val ex = intercept[AnalysisException] {
      DynamicInterpreter().interpret(dataset, inputDf).cacheIfNotCachedYet()
    }

    assert(ex.getMessage.contains("Path does not exist"))
  }

  test("Test non-existent mapping table directory handling in an experimental mapping rule") {
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      testCaseFactory.getTestCase(false, false, false, nonExistentTableMappingRule)

    val ex = intercept[AnalysisException] {
      DynamicInterpreter().interpret(dataset, inputDf).cacheIfNotCachedYet()
    }

    assert(ex.getMessage.contains("Path does not exist"))
  }

  test("Test empty mapping table error handling in a mapping rule") {
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      testCaseFactory.getTestCase(true, false, false, emptyTableMappingRule)

    val ex = intercept[RuntimeException] {
      DynamicInterpreter().interpret(dataset, inputDf).cacheIfNotCachedYet()
    }

    assert(ex.getMessage.contains("Unable to read the mapping table"))
  }

  test("Test empty mapping table error handling in an experimental mapping rule") {
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      testCaseFactory.getTestCase(false, false, false, emptyTableMappingRule)

    val ex = intercept[RuntimeException] {
      DynamicInterpreter().interpret(dataset, inputDf).cacheIfNotCachedYet()
    }

    assert(ex.getMessage.contains("Unable to read the mapping table"))
  }

}
