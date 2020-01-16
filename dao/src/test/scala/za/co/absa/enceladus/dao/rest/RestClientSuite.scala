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

import org.mockito.Mockito
import org.springframework.http._
import za.co.absa.enceladus.dao._

class RestClientSuite() extends RestClientBaseSuite {

  private val url = "url"
  private val requestBody = """{"test":"request"}"""
  private val responseJson = """{"test":"response"}"""

  "RestClient::authenticate" should {
    "call AuthClient::authenticate" in {
      restClient.authenticate()

      Mockito.verify(authClient, Mockito.only()).authenticate()
    }
    "update the authHeaders on successful authentication" in {
      val headers = new HttpHeaders()
      headers.add("test", "test")
      Mockito.when(authClient.authenticate()).thenReturn(headers)

      restClient.authenticate()

      getAuthHeaders(restClient) shouldBe headers
    }
  }

  private def getAuthHeaders(restClient: RestClient): HttpHeaders = {
    val field = classOf[RestClient].getDeclaredField("authHeaders")
    field.setAccessible(true)
    field.get(restClient).asInstanceOf[HttpHeaders]
  }

  "RestClient::sendGet" should {
    "return the entity on 200 OK" in {
      stubOkGetRequest(url, new HttpHeaders(), responseJson)

      val result = restClient.sendGet[String](url)
      result should be(responseJson)
    }

    "return 200 OK after successful retry on 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedGetRequest(url, expiredSessionHeaders)
      val renewedSessionHeaders = stubAuthSuccess()
      stubOkGetRequest(url, renewedSessionHeaders, responseJson)

      val result = restClient.sendGet[String](url)
      result should be(responseJson)
    }

    "throw an error on unsuccessful retry after 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedGetRequest(url, expiredSessionHeaders)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        restClient.sendGet[String](url)
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "return 200 OK after successful retry on 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenGetRequest(url, expiredSessionHeaders)
      val renewedSessionHeaders = stubAuthSuccess()
      stubOkGetRequest(url, renewedSessionHeaders, responseJson)

      val result = restClient.sendGet[String](url)
      result should be(responseJson)
    }

    "throw an error on unsuccessful retry after 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenGetRequest(url, expiredSessionHeaders)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        restClient.sendGet[String](url)
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "throw an error on 404 Not Found" in {
      stubNotFoundGetRequest(url)

      val exception = intercept[NotFoundException] {
        restClient.sendGet[String](url)
      }
      exception shouldBe NotFoundException("Entity not found - 404")
    }

    "throw an error on 500 Internal Server Error" in {
      stubInternalServerErrorGetRequest(url)

      val exception = intercept[DaoException] {
        restClient.sendGet[String](url)
      }
      exception shouldBe DaoException("Response - 500 : None")
    }
  }

  "RestClient::sendPost" should {
    "return true on 201 CREATED" in {
      stubCreatedPostRequest(url, new HttpHeaders(), requestBody, responseJson)

      val result = restClient.sendPost[String, String](url, requestBody)
      result should be(responseJson)
    }

    "return 201 CREATED after successful retry on 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestBody)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestBody, responseJson)

      val result = restClient.sendPost[String, String](url, requestBody)
      result should be(responseJson)
    }

    "throw an error on unsuccessful retry after 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestBody)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        restClient.sendPost[String, String](url, requestBody)
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "return 201 CREATED after successful retry on 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestBody)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestBody, responseJson)

      val result = restClient.sendPost[String, String](url, requestBody)
      result should be(responseJson)
    }

    "throw an error on unsuccessful retry after 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestBody)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        restClient.sendPost[String, String](url, requestBody)
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "throw an error on 404 Not Found" in {
      stubNotFoundPostRequest(url, requestBody)

      val exception = intercept[NotFoundException] {
        restClient.sendPost[String, String](url, requestBody)
      }
      exception shouldBe NotFoundException("Entity not found - 404")
    }

    "throw an error on 500 Internal Server Error" in {
      stubInternalServerErrorPostRequest(url, requestBody)

      val exception = intercept[DaoException] {
        restClient.sendPost[String, String](url, requestBody)
      }
      exception shouldBe DaoException("Response - 500 : None")
    }
  }

}
