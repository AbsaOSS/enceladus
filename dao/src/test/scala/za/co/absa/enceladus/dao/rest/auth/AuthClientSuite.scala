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

package za.co.absa.enceladus.dao.rest.auth

import org.mockito.stubbing.OngoingStubbing
import org.scalatest.matchers.should.Matchers
import org.mockito.scalatest.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfter
import org.springframework.http.{HttpStatus, HttpHeaders, ResponseEntity}
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.NotRetryableException.AuthenticationException
import za.co.absa.enceladus.dao.OptionallyRetryableException.ForbiddenException
import za.co.absa.enceladus.dao.rest.{ApiCaller, ApiCallerStub, AuthClient}

abstract class AuthClientSuite() extends AnyWordSpec
  with Matchers
  with MockitoSugar
  with BeforeAndAfter {

  protected val username: String = "user"
  protected val restTemplate: RestTemplate = mock[RestTemplate]
  protected val baseUrl: String = "http://localhost:8080/rest_api"
  protected val apiCaller: ApiCaller = new ApiCallerStub(baseUrl)

  val authClient: AuthClient

  def setUpSuccessfulAuthRequest(responseHeaders: LinkedMultiValueMap[String, String]): OngoingStubbing[ResponseEntity[String]]

  def setUpUnsuccessfulAuthRequest(httpStatus: HttpStatus): OngoingStubbing[ResponseEntity[String]]

  s"Calling authenticate()" should {
    "return authentication headers on 200 OK" in {
      val jwt = "jwt"

      val responseHeaders = new LinkedMultiValueMap[String, String]
      responseHeaders.add("jwt", jwt)

      setUpSuccessfulAuthRequest(responseHeaders)

      val expected = new HttpHeaders()
      expected.add("jwt", jwt)

      val response = authClient.authenticate()

      response should be(expected)
    }
    "throw an error on 403" in {
      setUpUnsuccessfulAuthRequest(HttpStatus.FORBIDDEN)

      val expectedErrMsg = s"Authentication failure (${HttpStatus.FORBIDDEN.value()}): user"
      val exception = intercept[ForbiddenException] {
        authClient.authenticate()
      }
      exception shouldBe ForbiddenException(expectedErrMsg)
    }
    "throw an error on anything other than 200 OK" in {
      setUpUnsuccessfulAuthRequest(HttpStatus.REQUEST_TIMEOUT)

      val expectedErrMsg = s"Authentication failure (${HttpStatus.REQUEST_TIMEOUT.value()}): user"
      val exception = intercept[AuthenticationException] {
        authClient.authenticate()
      }
      exception shouldBe AuthenticationException(expectedErrMsg)
    }
  }

}
