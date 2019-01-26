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
  * A representation of a finished/tested [[TestCase]].
  * @param id Unique identifier of the [[TestCaseResult]] and corresponding [[TestCase]]. Beware, uniqueness is not
  *           enforced or checked, this is up to the tester/user.
  * @param name Name of the test - only for readability purposes
  * @param diff The diff of the expected and actual output
  * @param passed A switch which tells us if the test passed or not
  * @param input Input from the [[TestCase]]
  * @param output Actual output from the test
  * @param expected Expected output by the [[TestCase]]
  * @param statusCode RestCall status code - what it is and what is should have been
  * @param elapsedTime Time elapsed on the run of the test
  */
case class TestCaseResult(id: String,
                          name: String,
                          diff: String,
                          passed: Boolean,
                          input: String,
                          output: String,
                          expected: String,
                          statusCode: String,
                          elapsedTime: Long)
