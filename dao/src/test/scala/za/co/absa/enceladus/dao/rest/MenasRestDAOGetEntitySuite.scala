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

package za.co.absa.enceladus.dao.rest

import org.scalatest.matchers.Matcher
import org.springframework.http._
import za.co.absa.enceladus.dao._

abstract class MenasRestDAOGetEntitySuite[T](methodName: String,
                                             url: String,
                                             entityJson: String) extends MenasRestDAOBaseSuite {

  def callMethod(): T

  def matchExpected(): Matcher[T]

  s"MenasRestDAO::$methodName" should {
    "return the entity on 200 OK" in {
      stubOkGetRequest(url, new HttpHeaders(), entityJson)

      val result = callMethod()
      result should matchExpected()
    }

    "return 200 OK after successful retry on 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedGetRequest(url, expiredSessionHeaders)
      val renewedSessionHeaders = stubAuthSuccess()
      stubOkGetRequest(url, renewedSessionHeaders, entityJson)

      val result = callMethod()
      result should matchExpected()
    }

    "throw an error on unsuccessful retry after 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedGetRequest(url, expiredSessionHeaders)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "return 200 OK after successful retry on 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenGetRequest(url, expiredSessionHeaders)
      val renewedSessionHeaders = stubAuthSuccess()
      stubOkGetRequest(url, renewedSessionHeaders, entityJson)

      val result = callMethod()
      result should matchExpected()
    }

    "throw an error on unsuccessful retry after 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenGetRequest(url, expiredSessionHeaders)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "throw an error on 404 Not Found" in {
      stubNotFoundGetRequest(url)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Entity not found - 404")
    }

    "throw an error on 500 Internal Server Error" in {
      stubInternalServerErrorGetRequest(url)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Response - 500 : None")
    }
  }

}
