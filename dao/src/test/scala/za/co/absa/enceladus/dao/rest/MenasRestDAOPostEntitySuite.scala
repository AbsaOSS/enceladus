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

import org.springframework.http.HttpHeaders
import za.co.absa.enceladus.dao.{DaoException, UnauthorizedException}

abstract class MenasRestDAOPostEntitySuite[T](methodName: String,
                                              url: String,
                                              requestBody: T) extends MenasRestDAOBaseSuite {

  def callMethod(): Boolean

  s"MenasRestDAO::$methodName" should {
    "return true on 201 CREATED" in {
      stubCreatedPostRequest(url, new HttpHeaders(), requestBody)

      val result = callMethod()
      result should be(true)
    }

    "return 201 CREATED after successful retry on 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestBody)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestBody)

      val result = callMethod()
      result should be(true)
    }

    "throw an error on unsuccessful retry after 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestBody)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "return 201 CREATED after successful retry on 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestBody)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestBody)

      val result = callMethod()
      result should be(true)
    }

    "throw an error on unsuccessful retry after 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestBody)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "throw an error on 404 Not Found" in {
      stubNotFoundPostRequest(url, requestBody)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Entity not found - 404")
    }

    "throw an error on 500 Internal Server Error" in {
      stubInternalServerErrorPostRequest(url, requestBody)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Response - 500 : None")
    }
  }

}
