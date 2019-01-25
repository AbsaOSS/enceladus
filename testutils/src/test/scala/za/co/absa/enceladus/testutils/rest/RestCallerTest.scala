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

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.enceladus.testutils.models.{POST, RestMethod, TestCase, TestCaseResult}

class RestCallerTest extends FunSuite with BeforeAndAfterEach {
  test("testRun") {
    val testCase: TestCase = TestCase("001",
      "Test001",
      "Post",
      "application/json",
      "https://httpbin.org/post",
      """{"form":{"foo":"baz","hello":"world"}}""",
      """{
        |  "args": {},
        |  "data": "{\"form\":{\"foo\":\"baz\",\"hello\":\"world\"}}",
        |  "files": {},
        |  "form": {},
        |  "headers": {
        |    "Accept": "*/*",
        |    "Accept-Encoding": "gzip, deflate",
        |    "Cache-Control": "no-cache",
        |    "Connection": "close",
        |    "Content-Length": "38",
        |    "Content-Type": "application/json",
        |    "Cookie": "",
        |    "Host": "httpbin.org",
        |    "Pragma": "no-cache",
        |    "User-Agent": "requests-scala"
        |  },
        |  "json": {
        |    "form": {
        |      "foo": "baz",
        |      "hello": "world"
        |    }
        |  },
        |  "url": "https://httpbin.org/post"
        |}""".stripMargin,
      "200")

    val testCaseResult: TestCaseResult = RestCaller.run(testCase)
    assert(testCaseResult.statusCode == "Should be 200, is 200")
    assert(testCaseResult.comparison.contains("\"op\": \"add\",\n  \"path\": \"/origin\","))
    assert(testCaseResult.elapsedTime > 0)
  }

  test("testCallPost") {
    val response = RestCaller.call(POST, "https://httpbin.org/post", """{"form":{"foo":"baz","hello":"world"}}""", "application/json")
    assert(response.text.contains(""""data": "{\"form\":{\"foo\":\"baz\",\"hello\":\"world\"}}""""))
    assert(response.text.contains(""""Content-Type": "application/json","""))
  }
}
