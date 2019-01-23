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

class TestRun(name: String) {
  val startTime: DateTime = DateTime.now(DateTimeZone.UTC)
  private[this] var endTime: Option[DateTime] = None: Option[DateTime]
  private[this] val testCaseResults: mutable.MutableList[TestCaseResult] = mutable.MutableList()

  def addTestCaseResult(testCaseResult: TestCaseResult): TestRun = {
    testCaseResults += testCaseResult
    this
  }

  def addTestCaseResults(testCaseResults: Array[TestCaseResult]): TestRun = {
    testCaseResults.foreach { addTestCaseResult }
    this
  }

  def endTestRun(): Unit = {
    if (endTime.isEmpty) { endTime = Some(DateTime.now(DateTimeZone.UTC)) }
  }

  def getEndTime: String = {
    endTime.getOrElse(s"TestRun $name not yet ended. Call #endTestRun to end it.").toString()
  }

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

  def getTestCaseResults: List[TestCaseResult] = { testCaseResults.toList }
}
