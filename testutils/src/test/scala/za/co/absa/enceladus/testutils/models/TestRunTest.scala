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

package za.co.absa.enceladus.testutils.models

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class TestRunTest extends FunSuite with BeforeAndAfterEach {

  var testRun: TestRun = _

  override def beforeEach() {
    testRun = new TestRun("UnitTests")
    testRun.addTestCaseResult(TestCaseResult("001", "FirstCall", "{}", true, "{}", "{}", "{}", "200", 1000))
    testRun.addTestCaseResult(TestCaseResult("002", "SecondCall", "{}", false, "{}", "{}", """{"a":"b"}""", "201", 5000))
  }

  test("testAddTestCaseResult") {
    testRun.addTestCaseResult(TestCaseResult("003", "ThirdCall", "{}", true, "{}", "{}", "{}", "200", 100))
    val results: Seq[TestCaseResult] = testRun.getTestCaseResults

    assert(results.size == 3)

    val addedResult = results(2)

    assert(addedResult.id == "003")
    assert(addedResult.name == "ThirdCall")
    assert(addedResult.diff == "{}")
    assert(addedResult.passed)
    assert(addedResult.input == "{}")
    assert(addedResult.output == "{}")
    assert(addedResult.expected == "{}")
    assert(addedResult.statusCode == "200")
    assert(addedResult.elapsedTime == 100)
  }

  test("testGetFinalInfo") {
    val finalInfo = testRun.getFinalInfo
    assert(finalInfo ==
      s"""{"name":"UnitTests","startTime":"${testRun.startTime}","endTime":"${testRun.getEndTime}",""" +
         """"numberOfUseCases":2,"numberOfPassedUseCases":1,"numberOfFailedUseCases":1}""")
  }

}
