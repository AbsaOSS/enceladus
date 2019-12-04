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

import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito
import org.mockito.stubbing.OngoingStubbing
import org.springframework.http._
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.UnauthorizedException

abstract class RestClientBaseSuite extends BaseTestSuite {

  val authClient: AuthClient = mock[AuthClient]
  val restTemplate: RestTemplate = mock[RestTemplate]

  val restClient: RestClient = new RestClient(authClient, restTemplate)

  before {
    Mockito.reset(authClient)
    Mockito.reset(restTemplate)
    setAuthHeaders(restClient, new HttpHeaders())
  }

  def stubOkGetRequest(url: String,
                       headers: HttpHeaders,
                       responseBody: String): OngoingStubbing[ResponseEntity[String]] = {
    stubGetRequest(url, headers)
      .thenReturn(new ResponseEntity[String](responseBody, HttpStatus.OK))
  }

  def stubCreatedPostRequest[T](url: String,
                                headers: HttpHeaders,
                                requestBody: T,
                                responseBody: String): OngoingStubbing[ResponseEntity[String]] = {
    stubPostRequest(url, headers, requestBody)
      .thenReturn(new ResponseEntity[String](responseBody, HttpStatus.CREATED))
  }

  def stubUnauthorizedGetRequest(url: String,
                                 headers: HttpHeaders): OngoingStubbing[ResponseEntity[String]] = {
    stubGetRequest(url, headers)
      .thenReturn(new ResponseEntity[String](HttpStatus.UNAUTHORIZED))
  }

  def stubUnauthorizedPostRequest[T](url: String,
                                     headers: HttpHeaders,
                                     requestBody: T): OngoingStubbing[ResponseEntity[String]] = {
    stubPostRequest(url, headers, requestBody)
      .thenReturn(new ResponseEntity[String](HttpStatus.UNAUTHORIZED))
  }

  def stubForbiddenGetRequest(url: String,
                              headers: HttpHeaders): OngoingStubbing[ResponseEntity[String]] = {
    stubGetRequest(url, headers)
      .thenReturn(new ResponseEntity[String](HttpStatus.FORBIDDEN))
  }

  def stubForbiddenPostRequest[T](url: String,
                                  headers: HttpHeaders,
                                  requestBody: T): OngoingStubbing[ResponseEntity[String]] = {
    stubPostRequest(url, headers, requestBody)
      .thenReturn(new ResponseEntity[String](HttpStatus.FORBIDDEN))
  }

  def stubNotFoundGetRequest(url: String): OngoingStubbing[ResponseEntity[String]] = {
    stubGetRequest(url)
      .thenReturn(new ResponseEntity[String](HttpStatus.NOT_FOUND))
  }

  def stubNotFoundPostRequest[T](url: String,
                                 requestBody: T): OngoingStubbing[ResponseEntity[String]] = {
    stubPostRequest(url, new HttpHeaders(), requestBody)
      .thenReturn(new ResponseEntity[String](HttpStatus.NOT_FOUND))
  }

  def stubInternalServerErrorGetRequest(url: String): OngoingStubbing[ResponseEntity[String]] = {
    stubGetRequest(url)
      .thenReturn(new ResponseEntity[String](HttpStatus.INTERNAL_SERVER_ERROR))
  }

  def stubInternalServerErrorPostRequest[T](url: String,
                                            requestBody: T): OngoingStubbing[ResponseEntity[String]] = {
    stubPostRequest(url, new HttpHeaders(), requestBody)
      .thenReturn(new ResponseEntity[String](HttpStatus.INTERNAL_SERVER_ERROR))
  }

  private def stubGetRequest(url: String,
                             headers: HttpHeaders = new HttpHeaders()): OngoingStubbing[ResponseEntity[String]] = {
    val request = new HttpEntity[String](headers)
    stubRequest(url, request, HttpMethod.GET)
  }

  private def stubPostRequest[T](url: String,
                                 headers: HttpHeaders,
                                 requestBody: T): OngoingStubbing[ResponseEntity[String]] = {
    headers.add(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
    val request = new HttpEntity[T](requestBody, headers)
    stubRequest(url, request, HttpMethod.POST)
  }

  private def stubRequest[T](url: String,
                             request: HttpEntity[T],
                             method: HttpMethod): OngoingStubbing[ResponseEntity[String]] = {
    Mockito
      .when(restTemplate.exchange(
        eqTo(url),
        eqTo(method),
        eqTo(request),
        eqTo(classOf[String])))
  }

  def stubAuthFailure(): Unit = {
    Mockito.when(authClient.authenticate()).thenThrow(UnauthorizedException("Authentication failure"))
  }

  def stubAuthSuccess(): HttpHeaders = {
    val renewedSessionHeaders = new HttpHeaders()
    renewedSessionHeaders.add("session", "renewed")
    Mockito.when(authClient.authenticate()).thenReturn(renewedSessionHeaders)

    renewedSessionHeaders
  }

  def stubExpiredSession(): HttpHeaders = {
    val expiredSessionHeaders = new HttpHeaders()
    expiredSessionHeaders.add("session", "expired")
    setAuthHeaders(restClient, expiredSessionHeaders)

    expiredSessionHeaders
  }

  private def setAuthHeaders(restClient: RestClient, httpHeaders: HttpHeaders): Unit = {
    val field = classOf[RestClient].getDeclaredField("authHeaders")
    field.setAccessible(true)
    field.set(restClient, httpHeaders)
  }

}
