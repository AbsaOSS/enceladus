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

package za.co.absa.enceladus.menas.integration.controllers

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.context.annotation.Bean
import org.springframework.http._
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.util.LinkedMultiValueMap
import za.co.absa.enceladus.menas.integration.repositories.BaseRepositoryTest

import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class BaseRestApiTest extends BaseRepositoryTest {

  import scala.concurrent.ExecutionContext.Implicits.global

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @LocalServerPort
  private val port = 0

  @Value("${menas.auth.inmemory.user}")
  val user: String = ""
  @Value("${menas.auth.inmemory.password}")
  val passwd: String = ""

  private lazy val baseUrl = s"http://localhost:$port/api"
  private lazy val authHeaders = getAuthHeaders(user, passwd)

  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  @Autowired
  private val restTemplate: TestRestTemplate = null

  @Bean
  def restTemplateBuilder: RestTemplateBuilder = {
    val messageConverter = new MappingJackson2HttpMessageConverter(objectMapper)
    val templateBuilder = new RestTemplateBuilder()
      .additionalMessageConverters(messageConverter)
    templateBuilder
  }

  def getAuthHeaders(username: String, password: String): HttpHeaders = {
    val loginUrl = s"$baseUrl/login?username=$username&password=$password&submit=Login"

    val response = restTemplate.postForEntity(loginUrl, HttpEntity.EMPTY, classOf[String])

    val cookie = response.getHeaders.get("set-cookie").get(0)
    val csrfToken = response.getHeaders.get("X-CSRF-TOKEN").get(0)
    val headers = new HttpHeaders()
    headers.add("cookie", cookie)
    headers.add("X-CSRF-TOKEN", csrfToken)
    headers
  }

  def sendGet[T](urlPath: String, headers: HttpHeaders = new HttpHeaders())
                (implicit ct: ClassTag[T]): ResponseEntity[T] = {
    send(HttpMethod.GET, urlPath, headers)
  }

  def sendGetAsync[T](urlPath: String, headers: HttpHeaders = new HttpHeaders())
                (implicit ct: ClassTag[T]): Future[ResponseEntity[T]] = {
    sendAsync(HttpMethod.GET, urlPath, headers)
  }

  def sendPost[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): ResponseEntity[T] = {
    send(HttpMethod.POST, urlPath, headers, bodyOpt)
  }

  def sendPostUploadFile[T](urlPath: String,
                            fileName: String,
                            parameters: Map[String, Any],
                            fileParamName: String = "file",
                            headers: HttpHeaders = new HttpHeaders())
                           (implicit ct: ClassTag[T]): ResponseEntity[T] = {
    upload(urlPath, headers, fileParamName, fileName, parameters)
  }

  def sendPostRemoteFile[T](urlPath: String,
                            parameters: Map[String, Any],
                            headers: HttpHeaders = new HttpHeaders())
                           (implicit ct: ClassTag[T]): ResponseEntity[T] = {
    require(parameters.keySet.contains("remoteUrl"), s"parameters map must contain the 'remoteUrl' entry, but only $parameters was found")

    fromRemote(urlPath, headers, parameters)
  }

  def sendPostAsync[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): Future[ResponseEntity[T]] = {
    sendAsync(HttpMethod.POST, urlPath, headers, bodyOpt)
  }

  def sendPut[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): ResponseEntity[T] = {
    send(HttpMethod.PUT, urlPath, headers, bodyOpt)
  }

  def sendPutAsync[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): Future[ResponseEntity[T]] = {
    sendAsync(HttpMethod.PUT, urlPath, headers, bodyOpt)
  }

  def sendDelete[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): ResponseEntity[T] = {
    send(HttpMethod.DELETE, urlPath, headers)
  }

  def sendDeleteAsync[B, T](urlPath: String, headers: HttpHeaders = new HttpHeaders(),
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): Future[ResponseEntity[T]] = {
    sendAsync(HttpMethod.DELETE, urlPath, headers)
  }

  def sendAsync[B, T](method: HttpMethod, urlPath: String, headers: HttpHeaders = HttpHeaders.EMPTY,
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): Future[ResponseEntity[T]] = {
    Future { send(method, urlPath, headers, bodyOpt) }
  }

  def send[B, T](method: HttpMethod, urlPath: String, headers: HttpHeaders = HttpHeaders.EMPTY,
                 bodyOpt: Option[B] = None)(implicit ct: ClassTag[T]): ResponseEntity[T] = {
    val url = s"$baseUrl/$urlPath"
    headers.addAll(authHeaders)
    headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
    val httpEntity = bodyOpt match {
      case Some(body) => new HttpEntity[B](body, headers)
      case None       => new HttpEntity[B](headers)
    }
    val clazz = ct.runtimeClass.asInstanceOf[Class[T]]

    restTemplate.exchange(url, method, httpEntity, clazz)
  }

  def upload[T](urlPath: String,
                headers: HttpHeaders = HttpHeaders.EMPTY,
                fileParamName: String,
                fileName: String,
                additionalParams: Map[String, Any])
               (implicit ct: ClassTag[T]): ResponseEntity[T] = {

    val parameters = new LinkedMultiValueMap[String, Any]
    parameters.add(fileParamName, new org.springframework.core.io.ClassPathResource(fileName))
    additionalParams.foreach {
      case (key, value) => parameters.add(key, value)
    }

    val url = s"$baseUrl/$urlPath"
    headers.addAll(authHeaders)
    headers.setContentType(MediaType.MULTIPART_FORM_DATA)

    val clazz = ct.runtimeClass.asInstanceOf[Class[T]]

    val httpEntity = new HttpEntity[LinkedMultiValueMap[String, Any]](parameters, headers)
    restTemplate.exchange(url, HttpMethod.POST, httpEntity, clazz)
  }

  def fromRemote[T](urlPath: String,
                    headers: HttpHeaders = HttpHeaders.EMPTY,
                    params: Map[String, Any])
               (implicit ct: ClassTag[T]): ResponseEntity[T] = {

    val parameters = new LinkedMultiValueMap[String, Any]
    params.foreach {
      case (key, value) => parameters.add(key, value) // TODO convert to Map<K, List<V>> and pass to LMVM directly?
    }

    val url = s"$baseUrl/$urlPath"
    headers.addAll(authHeaders)
    headers.setContentType(MediaType.MULTIPART_FORM_DATA) // no payload transfer besides the form data? application/x-www-form-urlencoded?

    val clazz = ct.runtimeClass.asInstanceOf[Class[T]]

    val httpEntity = new HttpEntity[LinkedMultiValueMap[String, Any]](parameters, headers)
    restTemplate.exchange(url, HttpMethod.POST, httpEntity, clazz)
  }


  def assertOk(responseEntity: ResponseEntity[_]): Unit = {
    assert(responseEntity.getStatusCode == HttpStatus.OK)
  }

  def assertBadRequest(responseEntity: ResponseEntity[_]): Unit = {
    assert(responseEntity.getStatusCode == HttpStatus.BAD_REQUEST)
  }

  def assertNotFound(responseEntity: ResponseEntity[_]): Unit = {
    assert(responseEntity.getStatusCode == HttpStatus.NOT_FOUND)
  }

  def assertCreated(responseEntity: ResponseEntity[_]): Unit = {
    assert(responseEntity.getStatusCode == HttpStatus.CREATED)
  }

}
