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

package za.co.absa.enceladus.dao.rest.auth

import org.mockito.Mockito
import org.mockito.stubbing.OngoingStubbing
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.util.LinkedMultiValueMap
import za.co.absa.enceladus.dao.rest.{AuthClient, LdapAuthClient}

class LdapAuthClientSuite extends AuthClientSuite {
  private val password = "password"

  private val requestParts = new LinkedMultiValueMap[String, String]
  requestParts.add("username", username)
  requestParts.add("password", password)

  override val authClient: AuthClient = new LdapAuthClient(username, password, restTemplate, apiCaller)

  override def setUpSuccessfulAuthRequest(responseHeaders: LinkedMultiValueMap[String, String]): OngoingStubbing[ResponseEntity[String]] = {
    Mockito
      .when(restTemplate.postForEntity(s"$baseUrl/api/login", requestParts, classOf[String]))
      .thenReturn(new ResponseEntity[String](responseHeaders, HttpStatus.OK))
  }

  override def setUpUnsuccessfulAuthRequest(): OngoingStubbing[ResponseEntity[String]] = {
    Mockito
      .when(restTemplate.postForEntity(s"$baseUrl/api/login", requestParts, classOf[String]))
      .thenReturn(new ResponseEntity[String](HttpStatus.FORBIDDEN))
  }

}
