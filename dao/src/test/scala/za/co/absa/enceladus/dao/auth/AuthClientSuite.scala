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

package za.co.absa.enceladus.dao.auth

import org.mockito.stubbing.OngoingStubbing
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.springframework.http.{HttpHeaders, ResponseEntity}
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.{AuthClient, UnauthorizedException}

abstract class AuthClientSuite() extends WordSpec
  with Matchers
  with MockitoSugar
  with BeforeAndAfter {

  protected val username: String = "user"
  protected val restTemplate: RestTemplate = mock[RestTemplate]
  protected val url: String = "http://localhost:8080/menas/api"

  val authClient: AuthClient

  def setUpSuccessfulAuthRequest(responseHeaders: LinkedMultiValueMap[String, String]): OngoingStubbing[ResponseEntity[String]]

  def setUpUnsuccessfulAuthRequest(): OngoingStubbing[ResponseEntity[String]]

  s"Calling authenticate()" should {
    "return authentication headers on 200 OK" in {
      val sessionCookie = "session-cookie"
      val csrfToken = "csrf-token"

      val responseHeaders = new LinkedMultiValueMap[String, String]
      responseHeaders.add("set-cookie", sessionCookie)
      responseHeaders.add("x-csrf-token", csrfToken)

      setUpSuccessfulAuthRequest(responseHeaders)

      val expected = new HttpHeaders()
      expected.add("cookie", sessionCookie)
      expected.add("x-csrf-token", csrfToken)

      val response = authClient.authenticate()

      response should be(expected)
    }
    "throw an error on anything other than 200 OK" in {
      setUpUnsuccessfulAuthRequest()

      val exception = intercept[UnauthorizedException] {
        authClient.authenticate()
      }
      exception shouldBe UnauthorizedException("Authentication failure (403): user")
    }
  }

}
