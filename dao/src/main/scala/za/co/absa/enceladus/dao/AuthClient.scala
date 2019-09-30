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

package za.co.absa.enceladus.dao

import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpHeaders, HttpStatus, ResponseEntity}
import org.springframework.security.kerberos.client.KerberosRestTemplate
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.menasplugin._

object AuthClient {

  def apply(credentials: MenasCredentials, apiBaseUrl: String): AuthClient = {
    credentials match {
      case menasCredentials: MenasPlainCredentials    => createLdapAuthClient(apiBaseUrl, menasCredentials)
      case menasCredentials: MenasKerberosCredentials => createSpnegoAuthClient(apiBaseUrl, menasCredentials)
      case InvalidMenasCredentials                    => throw UnauthorizedException("No Menas credentials provided")
    }
  }

  private def createLdapAuthClient(apiBaseUrl: String, credentials: MenasPlainCredentials): LdapAuthClient = {
    val restTemplate = RestTemplateSingleton.instance
    new LdapAuthClient(credentials.username, credentials.password, restTemplate, s"$apiBaseUrl/login")
  }

  private def createSpnegoAuthClient(apiBaseUrl: String, credentials: MenasKerberosCredentials): SpnegoAuthClient = {
    val restTemplate = new KerberosRestTemplate(credentials.keytabLocation, credentials.username)

    new SpnegoAuthClient(credentials.username, credentials.keytabLocation, restTemplate, s"$apiBaseUrl/user/info")
  }
}

sealed abstract class AuthClient(username: String, restTemplate: RestTemplate, url: String) {

  import collection.JavaConverters._

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[UnauthorizedException]
  def authenticate(): HttpHeaders = {
    val response = requestAuthentication()
    val statusCode = response.getStatusCode

    statusCode match {
      case HttpStatus.OK =>
        log.info(s"Authentication successful: $username")
        getAuthHeaders(response)
      case _             =>
        throw UnauthorizedException(s"Authentication failure ($statusCode): $username")
    }
  }

  protected def requestAuthentication(): ResponseEntity[String]

  private def getAuthHeaders(response: ResponseEntity[String]): HttpHeaders = {
    val headers = response.getHeaders
    val sessionCookie = headers.get("set-cookie").asScala.head
    val csrfToken = headers.get("X-CSRF-TOKEN").asScala.head

    log.info(s"Session Cookie: $sessionCookie")
    log.info(s"CSRF Token: $csrfToken")

    val resultHeaders = new HttpHeaders()
    resultHeaders.add("cookie", sessionCookie)
    resultHeaders.add("X-CSRF-TOKEN", csrfToken)
    resultHeaders
  }

}

class SpnegoAuthClient(username: String, keytabLocation: String, restTemplate: RestTemplate, url: String)
  extends AuthClient(username, restTemplate, url) {

  override protected def requestAuthentication(): ResponseEntity[String] = {
    log.info(s"Authenticating via SPNEGO: user '$username', with keytab '$keytabLocation'")
    restTemplate.getForEntity(url, classOf[String])
  }

}

class LdapAuthClient(username: String, password: String, restTemplate: RestTemplate, url: String)
  extends AuthClient(username, restTemplate, url) {

  override protected def requestAuthentication(): ResponseEntity[String] = {
    val requestParts = new LinkedMultiValueMap[String, String]
    requestParts.add("username", username)
    requestParts.add("password", password)

    log.info(s"Authenticating via LDAP: user '$username'")
    restTemplate.postForEntity(url, requestParts, classOf[String])
  }

}
