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

import org.mockito.Mockito
import org.mockito.stubbing.OngoingStubbing
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.util.LinkedMultiValueMap
import za.co.absa.enceladus.dao.rest.{AuthClient, SpnegoAuthClient}

class SpnegoAuthClientSuite extends AuthClientSuite {

  override val authClient: AuthClient = new SpnegoAuthClient(username, "user.keytab", restTemplate, apiCaller)

  override def setUpSuccessfulAuthRequest(responseHeaders: LinkedMultiValueMap[String, String]): OngoingStubbing[ResponseEntity[String]] = {
    Mockito
      .when(restTemplate.getForEntity(s"$baseUrl/api/user/info", classOf[String]))
      .thenReturn(new ResponseEntity[String](responseHeaders, HttpStatus.OK))
  }

  override def setUpUnsuccessfulAuthRequest(): OngoingStubbing[ResponseEntity[String]] = {
    Mockito
      .when(restTemplate.getForEntity(s"$baseUrl/api/user/info", classOf[String]))
      .thenReturn(new ResponseEntity[String](HttpStatus.FORBIDDEN))
  }

}
