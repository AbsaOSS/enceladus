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

/**
  * Representation of one test case to be executed.
  *
  * @param id Unique identifier of the [[TestCase]] and corresponding [[TestCaseResult]]. Beware, uniqueness is not
  *           enforced or checked, this is up to the tester/user.
  * @param name Name of the test - only for readability purposes
  * @param methodString REST call method as a string. Choices are post, get, put and update
  * @param contentType Generally supported content types for rest call
  * @param url REST call URL
  * @param payload Data to be passed with the call to the server
  * @param expectedOutput Expected output of this test case
  * @param expectedStatusCode Expected status code of the REST call response
  */
case class TestCase(id: String,
                    name: String,
                    methodString: String,
                    contentType: String,
                    url: String,
                    payload: String,
                    expectedOutput: String,
                    expectedStatusCode: String) {
  val method: RestMethod = methodString.toLowerCase match {
    case "post" => POST
    case "get" => GET
    case "put" => PUT
    case "delete" => DELETE
  }
}

