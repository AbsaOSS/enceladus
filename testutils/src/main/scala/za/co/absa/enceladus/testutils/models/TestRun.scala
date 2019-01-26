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

import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable

/**
  * Class representing one test run, a aggregation of test cases. This can be though of as one E2E
  * test or a bulk of smaller non blocking test cases. This class will hold the list of [[TestCaseResult]]
  * where evaluations of the previously run test cases can be found.
  *
  * @constructor create a new TestRun with name
  * @param name Name of the TestRun for users identification of the tests
  */
class TestRun(name: String) {
  val startTime: DateTime = DateTime.now(DateTimeZone.UTC)
  private[this] var endTime: Option[DateTime] = None: Option[DateTime]
  private[this] val testCaseResults: mutable.MutableList[TestCaseResult] = mutable.MutableList()

  /**
    * Add [[TestCaseResult]] to a [[TestRun]] instance.
    *
    * @param testCaseResult Instance of [[TestCaseResult]] to log/add to particular TestRun
    * @return Returns current instance of [[TestRun]]
    */
  def addTestCaseResult(testCaseResult: TestCaseResult): TestRun = {
    testCaseResults += testCaseResult
    this
  }

  /**
    * Add multiple [[TestCaseResult]]s to the current instance of [[TestRun]]
    *
    * @param testCaseResults Array of [[TestCaseResult]]s to log/add to particular TestRun
    * @return Returns current instance of [[TestRun]]
    */
  def addTestCaseResults(testCaseResults: Array[TestCaseResult]): TestRun = {
    testCaseResults.foreach { addTestCaseResult }
    this
  }

  /**
    * Ends/Finishes test run. Which means it logs the end time.
    */
  def endTestRun(): Unit = {
    if (endTime.isEmpty) { endTime = Some(DateTime.now(DateTimeZone.UTC)) }
  }

  /**
    * Get the endTime value of the current instance. If the test run wasn't finished yet, returns String message.
    * @return Returns default for [[org.joda.time.DateTime]] toString or a message
    */
  def getEndTime: String = {
    endTime.getOrElse(s"TestRun $name not yet ended. Call #endTestRun to end it.").toString()
  }

  /**
    * Returns pretty formatted json string of information gathered by the [[TestRun]] instance. That is:
    * - name
    * - start time
    * - end time
    * - number of usecases ran
    * - number of usecases failed
    * - number of usecases passed
    * @return
    */
  def getFinalInfo: String = {
    endTestRun()
    val startTimeString = this.startTime.toString()
    val endTimeString = this.endTime.get.toString()
    val numOfPassedUseCases = testCaseResults.count(_.passed)
    val numOfFailedUseCases = testCaseResults.count(!_.passed)
    val numOfUseCases = numOfFailedUseCases + numOfPassedUseCases

    val jsonObj = ujson.Obj("name" -> this.name,
                            "startTime" -> startTimeString,
                            "endTime" -> endTimeString,
                            "numberOfUseCases" -> numOfUseCases,
                            "numberOfPassedUseCases" -> numOfPassedUseCases,
                            "numberOfFailedUseCases" -> numOfFailedUseCases)

    ujson.write(jsonObj)
  }

  /**
    * @return Returns a immutable [[List]] of [[TestCaseResult]]s ran under current instance
    */
  def getTestCaseResults: List[TestCaseResult] = { testCaseResults.toList }
}
