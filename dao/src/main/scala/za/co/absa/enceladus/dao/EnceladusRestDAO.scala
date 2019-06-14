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

import java.net.URI

import scala.util.control.NonFatal

import org.apache.http.HttpStatus
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.springframework.http.{HttpStatus => SpringHttpStatus}
import org.springframework.security.kerberos.client.KerberosRestTemplate

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory

import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.MappingTable
import sun.security.krb5.internal.ktab.KeyTab

object EnceladusRestDAO extends EnceladusDAO {
  val conf = ConfigFactory.load()
  val restBase = conf.getString("menas.rest.uri")
  
  private var _userName: String = ""
  def userName = _userName
  var sessionCookie: String = ""
  var csrfToken: String = ""

  private val log = LogManager.getLogger("enceladus.conformance.EnceladusRestDAO")

  val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private[dao] def getUserFromKeytab(keytabLocation: String): String = {
    val keytab = KeyTab.getInstance(keytabLocation)
    keytab.getOneName.getName
  }
    
  def spnegoLogin(keytabLocation: String): Boolean = {
    import scala.collection.JavaConversions._
    
    val principal = getUserFromKeytab(keytabLocation)
    _userName = principal
    
    val template = new KerberosRestTemplate(keytabLocation, principal)
    val url = s"$restBase/user/info"

    log.info(s"Calling REST with spnego auth $principal $keytabLocation")
    val response = template.getForEntity(new URI(url), classOf[String])

    val status = response.getStatusCode
    val ok = status == SpringHttpStatus.OK
    val unAuthorized = status == SpringHttpStatus.UNAUTHORIZED
    log.info(response.toString)
    log.debug(s"OK: $ok")
    log.debug(s"Unauthorized: $unAuthorized")
    if (ok) {
      val headers = response.getHeaders
      sessionCookie = headers.get("set-cookie").head
      log.debug(s"Session cookie: $sessionCookie")
      csrfToken = headers.get("X-CSRF-TOKEN").head
      log.debug(s"CSRF Token: $csrfToken")

      log.info(s"Login Successful")
      log.info(s"Session Cookie: $sessionCookie")
      log.info(s"CSRF Token: $csrfToken")
    } else if (unAuthorized) {
      throw new UnauthorizedException
    } else {
      log.warn(response.toString)
    }
    ok
  }

  def postLogin(username: String, password: String) = {
    _userName = username
    try {
      val httpClient = HttpClients.createDefault
      val url = s"$restBase/login?username=${encode(username)}&password=${encode(password)}&submit=Login"
      val httpPost = new HttpPost(url)

      val response: CloseableHttpResponse = httpClient.execute(httpPost)
      try {
        val status = response.getStatusLine.getStatusCode
        val ok = status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES
        val unAuthorized = status == HttpStatus.SC_UNAUTHORIZED
        if (ok) {
          val cookieHeader = response.getFirstHeader("set-cookie")
          sessionCookie = cookieHeader.getValue

          val csrfHeader = response.getFirstHeader("X-CSRF-TOKEN")
          csrfToken = csrfHeader.getValue

          log.info(response.toString)
        } else if (unAuthorized) {
          throw new UnauthorizedException
        } else {
          log.warn(response.toString)
        }
        ok
      } finally {
        response.close()
      }
    } catch {
      case unAuthException: UnauthorizedException => throw unAuthException
      case NonFatal(e) =>
        log.error(s"Unable to login to Menas with error: ${e.getMessage}")
        false
    }
  }

  override def getDataset(name: String, version: Int): Dataset = {
    val url = s"$restBase/dataset/detail/${encode(name)}/$version"
    log.info(url)
    val json = authorizeGetRequest(url)
    log.info(json)
    objectMapper.readValue(json, classOf[Dataset])
  }

  override def getMappingTable(name: String, version: Int): MappingTable = {
    val url = s"$restBase/mappingTable/detail/${encode(name)}/$version"
    log.info(url)
    val json = authorizeGetRequest(url)
    log.info(json)
    objectMapper.readValue(json, classOf[MappingTable])
  }

  override def getSchema(name: String, version: Int): StructType = {
    val url = s"$restBase/schema/json/${encode(name)}/$version"
    log.info(url)
    val json = authorizeGetRequest(url)
    log.info(json)
    DataType.fromJson(json).asInstanceOf[StructType]
  }

  /* The URLEncoder implements the HTML Specifications
   * so have to replace '+' with %20
   * https://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
   */
  private def encode(string: String): String = {
    java.net.URLEncoder.encode(string, "UTF-8").replace("+", "%20")
  }

  private def authorizeGetRequest(url: String): String = {
    try {
      log.info(s"URL: $url GET")
      val httpClient = HttpClientBuilder.create().build()
      val httpGet = new HttpGet(url)
      httpGet.addHeader("cookie", sessionCookie)
      val response: CloseableHttpResponse = httpClient.execute(httpGet)
      try {
        val status = response.getStatusLine.getStatusCode
        val ok = status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES
        val unAuthorized = status == HttpStatus.SC_UNAUTHORIZED
        val content = {
          if (ok) {
            readResponseObject(response)
          } else if (unAuthorized) {
            throw new UnauthorizedException
          } else {
            throw new DaoException(s"Server returned HTTP response code: $status for Menas URL:  $url ")
          }
        }
        content.getOrElse("")
      } finally {
        response.close()
      }
    } catch {
      case unAuthException: UnauthorizedException => throw unAuthException
      case daoException: DaoException             => throw daoException
      case NonFatal(ex) =>
        log.error(s"Unable to connect to Menas endpoint via $url with error: ${ex.getMessage}")
        throw ex
    }
  }

  private def readResponseObject(response: CloseableHttpResponse): Option[String] = {
    val httpEntity = response.getEntity()
    if (httpEntity != null)
      Some(EntityUtils.toString(httpEntity))
    else {
      log.warn(response.toString)
      None
    }
  }

}
