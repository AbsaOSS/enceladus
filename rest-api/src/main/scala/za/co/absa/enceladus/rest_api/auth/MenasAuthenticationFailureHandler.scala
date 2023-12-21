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

package za.co.absa.enceladus.rest_api.auth

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.security.core.AuthenticationException
import org.springframework.security.web.authentication.AuthenticationFailureHandler
import org.springframework.stereotype.Component
import za.co.absa.enceladus.rest_api.auth.exceptions.{AuthHostTimeoutException, BadKrbHostException, BadLdapHostException}

/**
  * This class is responsible for mapping authentication exceptions to status codes in HTTP responses.
  */
@Component
class MenasAuthenticationFailureHandler extends AuthenticationFailureHandler {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def onAuthenticationFailure(request: HttpServletRequest,
                                       response: HttpServletResponse,
                                       ex: AuthenticationException): Unit = {
    log.warn("Authentication exception", ex)
    ex match {
      case _: AuthHostTimeoutException =>
        response.sendError(HttpStatus.GATEWAY_TIMEOUT.value, ex.getMessage)
      case _: BadKrbHostException | _: BadLdapHostException =>
        response.sendError(HttpStatus.BAD_GATEWAY.value, ex.getMessage)
      case _ =>
        response.sendError(HttpStatus.UNAUTHORIZED.value, ex.getMessage)
    }
  }
}
