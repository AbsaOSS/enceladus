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

package za.co.absa.enceladus.dao.rest

import za.co.absa.enceladus.dao.MenasException

class MenasRetryableExceptionsStringParserSuite extends BaseTestSuite {

  "MenasRetryableExceptionsStringParser::parse" should {
    "parse a single retryable exception" when {
      "it is a single 404" in {
        val inputExceptionsString = "404"
        val expectedExceptionsSet = Set(new MenasException("Optionally retryable exception - 404", None.orNull) {})

        val result = MenasRetryableExceptionsStringParser.parse(inputExceptionsString)

        val areEqual = result.toList.map(_.getMessage).sorted == expectedExceptionsSet.toList.map(_.getMessage).sorted
        areEqual should be(true)
      }

      "it is duplicated 404" in {
        val inputExceptionsString = "404;404"
        val expectedExceptionsSet = Set(new MenasException("Optionally retryable exception - 404", None.orNull) {})

        val result = MenasRetryableExceptionsStringParser.parse(inputExceptionsString)

        val areEqual = result.toList.map(_.getMessage).sorted == expectedExceptionsSet.toList.map(_.getMessage).sorted
        areEqual should be(true)
      }
    }

    "parse multiple retryable exceptions" when {
      "it is 400;404" in {
        val inputExceptionsString = "400;404"
        val expectedExceptionsSet = Set(
          new MenasException("Optionally retryable exception - 400", None.orNull) {},
          new MenasException("Optionally retryable exception - 404", None.orNull) {}
        )

        val result = MenasRetryableExceptionsStringParser.parse(inputExceptionsString)

        val areEqual = result.toList.map(_.getMessage).sorted == expectedExceptionsSet.toList.map(_.getMessage).sorted
        areEqual should be(true)
      }
    }

    "parse an empty retryable exception" when {
      "it is empty string" in {
        val inputExceptionsString = ""
        val expectedExceptionsSet = Set[MenasException]()

        val result = MenasRetryableExceptionsStringParser.parse(inputExceptionsString)

        result should be (expectedExceptionsSet)
      }

      "it is string with empty character" in {
        val inputExceptionsString = " "
        val expectedExceptionsSet = Set[MenasException]()

        val result = MenasRetryableExceptionsStringParser.parse(inputExceptionsString)

        result should be (expectedExceptionsSet)
      }
    }
  }
}
