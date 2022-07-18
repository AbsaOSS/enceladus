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

import org.slf4j.LoggerFactory
import org.springframework.http._
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.NotRetryableException.AuthenticationException
import za.co.absa.enceladus.dao.OptionallyRetryableException
import za.co.absa.enceladus.dao.RetryableException._
import za.co.absa.enceladus.dao.OptionallyRetryableException._

import scala.annotation.tailrec
import scala.reflect.ClassTag

protected class RestClient(authClient: AuthClient,
                           restTemplate: RestTemplate) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private var authHeaders = new HttpHeaders()

  def authenticate(): Unit = {
    authHeaders = authClient.authenticate()
  }

  @throws[DaoException]
  @throws[NotFoundException]
  @throws[UnauthorizedException]
  @throws[ForbiddenException]
  @throws[AuthenticationException]
  def sendGet[T](url: String)
                (implicit ct: ClassTag[T]): T = {
    send[T, T](HttpMethod.GET, url, new HttpHeaders())
  }

  @throws[DaoException]
  @throws[NotFoundException]
  @throws[UnauthorizedException]
  @throws[ForbiddenException]
  @throws[AuthenticationException]
  def sendPost[B, T](url: String,
                     requestBody: B)
                    (implicit ct: ClassTag[T]): T = {
    val headers = new HttpHeaders()
    headers.add(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
    send[B, T](HttpMethod.POST, url, headers, Option(requestBody))
  }

  @tailrec
  private def send[B, T](method: HttpMethod,
                         url: String,
                         headers: HttpHeaders = new HttpHeaders(),
                         bodyOpt: Option[B] = None,
                         retriesLeft: Int = 1)
                        (implicit ct: ClassTag[T]): T = {
    log.info(s"$method - URL: $url")
    headers.putAll(authHeaders)

    val httpEntity = bodyOpt match {
      case Some(body) =>
        val requestBody = JsonSerializer.toJson(body)
        log.info(s"Request Body: $requestBody")
        new HttpEntity[String](requestBody, headers)
      case None       =>
        new HttpEntity[String](headers)
    }

    val response = restTemplate.exchange(url, method, httpEntity, classOf[String])

    val statusCode = response.getStatusCode

    /**
     * This function handles unauthorized response by trying to authenticate
     * (if there are still some retries attempt left) - this might be due to an expired session.
     *
     * @param exceptionToThrow Instance of an exception that should be thrown if there are no retries left.
     */
    def handleUnauthorizedResponse(exceptionToThrow: OptionallyRetryableException): Unit = {
      log.warn(s"Response - ${statusCode} : ${Option(response.getBody).getOrElse("None")}")
      log.warn(s"Unauthorized $method request for Menas URL: $url")
      if (retriesLeft <= 0) {
        throw exceptionToThrow
      }

      log.warn(s"Expired session, reauthenticating")
      authenticate()

      log.info(s"Retrying $method request for Menas URL: $url")
      log.info(s"Retries left: $retriesLeft")
    }

    statusCode match {
      case HttpStatus.OK | HttpStatus.CREATED             =>
        log.info(s"Response (${response.getStatusCode}): ${response.getBody}")
        JsonSerializer.fromJson[T](response.getBody)

      case HttpStatus.UNAUTHORIZED =>
        handleUnauthorizedResponse(UnauthorizedException("Unauthorized, unable to reauthenticate, no retries left"))
        send[B, T](method, url, headers, bodyOpt, retriesLeft - 1)

      case HttpStatus.FORBIDDEN =>
        handleUnauthorizedResponse(ForbiddenException("Forbidden, unable to reauthenticate, no retries left"))
        send[B, T](method, url, headers, bodyOpt, retriesLeft - 1)

      case HttpStatus.NOT_FOUND                           =>
        throw NotFoundException(s"Entity not found - $statusCode")

      case _                                              =>
        throw DaoException(s"Response - $statusCode : ${Option(response.getBody).getOrElse("None")}")
    }
  }
}
