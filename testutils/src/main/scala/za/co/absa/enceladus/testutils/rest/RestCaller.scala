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

package za.co.absa.enceladus.testutils.rest

import za.co.absa.enceladus.testutils.models.RestMethod._
import requests.Response
import gnieh.diffson.sprayJson._
import za.co.absa.enceladus.testutils.models.{RestMethod, TestCase, TestCaseResult}

import scala.annotation.switch


object RestCaller {

  def run(testCase: TestCase): TestCaseResult = {
    lazy val callOutput = call(testCase.method, testCase.url, testCase.payload, testCase.contentType)
    val time = calculateTime { callOutput }
    val diff = compareJsons(testCase.expectedOutput, callOutput.text)
    val statusCodeOk = callOutput.statusCode.toString == testCase.expectedStatusCode
    val statusCodeMessage = s"Should be ${testCase.expectedStatusCode}, is ${callOutput.statusCode}"
    val passed = diff.isEmpty && statusCodeOk

    TestCaseResult(testCase.id, testCase.name, diff, passed, testCase.payload,
      callOutput.text, testCase.expectedOutput, statusCodeMessage, time)
  }

  def call(method: RestMethod, url: String, payload: String, contentType: String): Response = {
    val headers: Map[String, String] = Map("content-type" -> contentType)
    (method: @switch) match {
      case RestMethod.POST => callPost(url, payload, headers)
      case RestMethod.GET => callGet(url, payload, headers)
      case RestMethod.DELETE => callDelete(url, payload, headers)
      case RestMethod.PUT => callPut(url, payload, headers)
    }
  }

  private def callPost(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.post(url, data = payload, headers = headers)
  }

  private def callGet(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.get(url, data = payload, headers = headers)
  }

  private def callDelete(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.delete(url, data = payload, headers = headers)
  }

  private def callPut(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.put(url, data = payload, headers = headers)
  }

  /**
    * Compares two Strings of JSON standard compliant data
    * @param expectedJson JSON that is expected
    * @param actualJson JSON that was returned
    * @return Returns a diff of the two JSONs
    */
  private def compareJsons(expectedJson: String, actualJson: String): String = {
    JsonDiff.diff(expectedJson, actualJson, remember = false).toString
  }

  /**
    * Calculates times that it took for passed block of code to execute and finish.
    * @param callback Block of code, to be executed
    * @tparam A A type parameter specifying callback return
    * @return Returns a millisecond difference between start and end time
    */
  private def calculateTime[A](callback: => A): Long = {
    val startTime = System.nanoTime()
    callback
    val endTime = System.nanoTime()
    endTime - startTime
  }
}
