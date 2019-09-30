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

import org.slf4j.LoggerFactory
import org.springframework.http._
import org.springframework.web.client.RestTemplate

import scala.annotation.tailrec
import scala.reflect.ClassTag

protected class RestClient(authClient: AuthClient,
                           restTemplate: RestTemplate) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private var authHeaders: HttpHeaders = new HttpHeaders()

  def authenticate(): Unit = {
    authHeaders = authClient.authenticate()
  }

  def sendGet[T](urlPath: String)
                (implicit ct: ClassTag[T]): T = {
    send[T, T](HttpMethod.GET, urlPath, new HttpHeaders())
  }

  def sendPost[B, T](urlPath: String,
                     requestBody: B)
                    (implicit ct: ClassTag[T]): T = {
    val headers = new HttpHeaders()
    headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
    send[B, T](HttpMethod.POST, urlPath, headers, Option(requestBody))
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
      case Some(body) => new HttpEntity[B](body, headers)
      case None => new HttpEntity[B](headers)
    }

    val response = restTemplate.exchange(url, method, httpEntity, classOf[String])

    val statusCode = response.getStatusCode

    statusCode match {
      case HttpStatus.OK | HttpStatus.CREATED =>
        log.info(s"Response (${response.getStatusCode}): ${response.getBody}")
        JsonSerializer.fromJson[T](response.getBody)

      case HttpStatus.UNAUTHORIZED | HttpStatus.FORBIDDEN =>
        log.warn(s"Response - $statusCode : ${Option(response.getBody).getOrElse("None")}")
        log.warn(s"Unauthorized $method request for Menas URL: $url")
        if (retriesLeft <= 0) {
          throw UnauthorizedException("Unable to reauthenticate, no retries left")
        }

        log.warn(s"Expired session, reauthenticating")
        authenticate()

        log.info(s"Retrying $method request for Menas URL: $url")
        log.info(s"Retries left: $retriesLeft")
        send[B, T](method, url, headers, bodyOpt, retriesLeft - 1)

      case HttpStatus.NOT_FOUND =>
        throw DaoException(s"Entity not found - $statusCode")

      case _ =>
        throw DaoException(s"Response - $statusCode : ${Option(response.getBody).getOrElse("None")}")
    }
  }

}
