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

import requests.Response
import gnieh.diffson.sprayJson._
import za.co.absa.enceladus.testutils.HelperFunctions
import za.co.absa.enceladus.testutils.models._

import scala.annotation.switch

/**
  * Object created for easier use and run of REST API tests. Providing many methods making testers life easier.
  */
object RestCaller {

  /**
    * Runs, evaluates and returns the result of a REST API call.
    * @param testCase [[TestCase]] instance with information about the test to be ran and evaluated
    * @return Returns a result in a form of [[TestCaseResult]]
    */
  def run(testCase: TestCase): TestCaseResult = {
    val (time, callOutput) = HelperFunctions.calculateTime {
      call(testCase.method, testCase.url, testCase.payload, testCase.contentType)
    }
    val diff = compareJsons(testCase.expectedOutput, callOutput.text)
    val statusCodeOk = callOutput.statusCode.toString == testCase.expectedStatusCode
    val statusCodeMessage = s"Should be ${testCase.expectedStatusCode}, is ${callOutput.statusCode}"
    val passed = diff.isEmpty && statusCodeOk

    TestCaseResult(testCase.id, testCase.name, diff, passed, testCase.payload,
      callOutput.text, testCase.expectedOutput, statusCodeMessage, time)
  }

  /**
    * Calls a specified REST API method
    * @param method Method type to be called. POST, GET, PUT, UPDATE
    * @param url Url of the call
    * @param payload Data to be sent with the call
    * @param contentType Content type of the payload
    * @return Returns a response from the REST API call, which can be later evaluated
    */
  def call(method: RestMethod, url: String, payload: String, contentType: String): Response = {
    val headers: Map[String, String] = Map("content-type" -> contentType)
    (method: @switch) match {
      case POST => callPost(url, payload, headers)
      case GET => callGet(url, payload, headers)
      case DELETE => callDelete(url, payload, headers)
      case PUT => callPut(url, payload, headers)
    }
  }

  /**
    * Executes POST REST API call
    * @param url Url of the call
    * @param payload Data to be sent with the call
    * @param headers Headers for the call
    * @return Returns a response from the REST API call, which can be later evaluated
    */
  private def callPost(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.post(url, data = payload, headers = headers)
  }

  /**
    * Executes GET REST API call
    * @param url Url of the call
    * @param payload Data to be sent with the call
    * @param headers Headers for the call
    * @return Returns a response from the REST API call, which can be later evaluated
    */
  private def callGet(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.get(url, data = payload, headers = headers)
  }

  /**
    * Executes DELETE REST API call
    * @param url Url of the call
    * @param payload Data to be sent with the call
    * @param headers Headers for the call
    * @return Returns a response from the REST API call, which can be later evaluated
    */
  private def callDelete(url: String, payload: String, headers: Map[String, String]): Response = {
    requests.delete(url, data = payload, headers = headers)
  }

  /**
    * Executes PUT REST API call
    * @param url Url of the call
    * @param payload Data to be sent with the call
    * @param headers Headers for the call
    * @return Returns a response from the REST API call, which can be later evaluated
    */
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
}
