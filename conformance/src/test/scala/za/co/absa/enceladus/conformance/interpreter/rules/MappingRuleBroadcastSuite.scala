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

import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.SimpleTestCaseFactory.simpleMappingRule
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class MappingRuleBroadcastSuite extends FunSuite with SparkTestBase with LoggerTestBase with BeforeAndAfterAll {
  private val testCaseFactory = new SimpleTestCaseFactory()

  override def beforeAll(): Unit = {
    super.beforeAll()
    testCaseFactory.createMappingTables()
  }

  override def afterAll(): Unit = {
    testCaseFactory.deleteMappingTables()
    super.afterAll()
  }

  test("Test broadcasting mapping rule works on a simple mapping rule (no structs or arrays)") {
    implicit val (inputDf, dataset, dao, progArgs, featureSwitches) =
      testCaseFactory.getTestCase(true, simpleMappingRule)

    val df = DynamicInterpreter.interpret(dataset, inputDf).cache

    // Make sure the schema is correct
    assert(df.schema.fields.length == 6)
    assert(df.schema.fields.exists(_.name == "id"))
    assert(df.schema.fields.exists(_.name == "int_num"))
    assert(df.schema.fields.exists(_.name == "long_num"))
    assert(df.schema.fields.exists(_.name == "str_val"))
    assert(df.schema.fields.exists(_.name == "errCol"))
    assert(df.schema.fields.exists(_.name == "conformedIntNum"))

    // Make sure the number of errors is as expected
    assert(df.filter(size(col("errCol")) === 0).count == 3)
    assert(df.filter(size(col("errCol")) !== 0).count == 2)
  }

}
