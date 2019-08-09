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
import org.apache.http.{HttpEntity, HttpStatus}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.springframework.http.{HttpStatus => SpringHttpStatus}
import org.springframework.security.kerberos.client.KerberosRestTemplate
import org.springframework.web.client.RestTemplate
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import sun.security.krb5.internal.ktab.KeyTab //scalastyle:ignore illegal.imports
import za.co.absa.enceladus.dao.menasplugin.MenasCredentials
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.MappingTable
import org.springframework.util.LinkedMultiValueMap

object EnceladusRestDAO extends EnceladusDAO {
  private val log = LogManager.getLogger(this.getClass)

  private val conf = ConfigFactory.load()
  private val restBase = conf.getString("menas.rest.uri")

  val httpClient: CloseableHttpClient = HttpClients.createDefault

  private var _userName: String = ""
  def userName: String = _userName

  private var _sessionCookie: String = ""
  def sessionCookie: String = _sessionCookie

  private var _csrfToken: String = ""
  def csrfToken: String = _csrfToken

  var login: Option[Either[MenasCredentials, String]] = None

  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private[dao] def getUserFromKeytab(keytabLocation: String): String = {
    val keytab = KeyTab.getInstance(keytabLocation)
    keytab.getOneName.getName
  }

  def enceladusLogin(): Boolean = {
    login match {
      case Some(creds) => creds match {
        case Left(userPassCreds: MenasCredentials) => EnceladusRestDAO.postLogin(userPassCreds.username, userPassCreds.password)
        case Right(keytabLocation: String)         => EnceladusRestDAO.spnegoLogin(keytabLocation)
      }
      case None => throw UnauthorizedException("Menas credentials have to be provided")
    }
  }

  private def restTemplateLogin(restTemplate: RestTemplate, url: String, username: String,
      reqBody: Option[LinkedMultiValueMap[String, String]] = None): Boolean = {
    import scala.collection.JavaConversions._

    _userName = username

    val response = reqBody match {
      case Some(params) => restTemplate.postForEntity(new URI(url), params, classOf[String])
      case None => restTemplate.getForEntity(new URI(url), classOf[String])
    }

    response.getStatusCode match {
      case SpringHttpStatus.OK => {
        val headers = response.getHeaders
        _sessionCookie = headers.get("set-cookie").head
        _csrfToken = headers.get("X-CSRF-TOKEN").head

        log.info(s"Login Successful")
        log.info(s"Session Cookie: $sessionCookie")
        log.info(s"CSRF Token: $csrfToken")
        true
      }
      case SpringHttpStatus.UNAUTHORIZED => {
        throw UnauthorizedException()
      }
      case resp => {
        log.warn(response.toString)
        false
      }
    }
  }

  private def spnegoLogin(keytabLocation: String): Boolean = {
    val username = getUserFromKeytab(keytabLocation)
    val template = new KerberosRestTemplate(keytabLocation, username)
    val url = s"$restBase/user/info"
    log.info(s"Calling REST with SPNEGO auth $username $keytabLocation")
    restTemplateLogin(template, url, username)
  }

  private def postLogin(username: String, password: String): Boolean = {
    val parts = new LinkedMultiValueMap[String, String]
    parts.add("username", username)
    parts.add("password", password)

    val url = s"$restBase/login"
    val template = new RestTemplate()
    restTemplateLogin(template, url, username, Some(parts))
  }

  override def getDataset(name: String, version: Int): Dataset = {
    val url = s"$restBase/dataset/detail/${encode(name)}/$version"
    log.info(url)
    val json = sendGet(url)
    log.info(json)
    objectMapper.readValue(json, classOf[Dataset])
  }

  override def getMappingTable(name: String, version: Int): MappingTable = {
    val url = s"$restBase/mappingTable/detail/${encode(name)}/$version"
    log.info(url)
    val json = sendGet(url)
    log.info(json)
    objectMapper.readValue(json, classOf[MappingTable])
  }

  override def getSchema(name: String, version: Int): StructType = {
    val url = s"$restBase/schema/json/${encode(name)}/$version"
    log.info(url)
    val json = sendGet(url)
    log.info(json)
    DataType.fromJson(json).asInstanceOf[StructType]
  }

  override def getSchemaAttachment(name: String, version: Int): String = {
    val url = s"$restBase/schema/export/${encode(name)}/$version"
    log.info(url)
    sendGet(url)
  }

  /* The URLEncoder implements the HTML Specifications
   * so have to replace '+' with %20
   * https://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
   */
  private def encode(string: String): String = {
    java.net.URLEncoder.encode(string, "UTF-8").replace("+", "%20")
  }

  private def sendGet(url: String, retriesLeft: Int = 1): String = {
    try {
      log.info(s"URL: $url GET")
      val httpGet = new HttpGet(url)
      httpGet.addHeader("cookie", sessionCookie)
      val response: CloseableHttpResponse = httpClient.execute(httpGet)
      try {
        val status = response.getStatusLine.getStatusCode
        val ok = status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES
        val unAuthorized = status == HttpStatus.SC_UNAUTHORIZED

        if (unAuthorized) {
          log.warn(s"Unauthorized GET request for Menas URL: $url")
          log.warn(s"Expired session, reauthenticating")
          if (enceladusLogin()) {
            if (retriesLeft > 0) {
              throw UnauthorizedException(s"Unable to reauthenticate after retries")
            }
            log.info(s"Retrying GET request for Menas URL: $url")
            log.info(s"Retries left: $retriesLeft")
            sendGet(url, retriesLeft - 1)
          } else {
            throw UnauthorizedException()
          }
        } else {
          val content = {
            if (ok) {
              readResponseObject(response)
            } else {
              throw DaoException(s"Server returned HTTP response code: $status for Menas URL: $url")
            }
          }
          content.getOrElse("")
        }
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
    val httpEntity: Option[HttpEntity] = Option(response.getEntity)
    if (httpEntity.isEmpty) {
      log.warn(response.toString)
    }
    httpEntity.map(EntityUtils.toString)
  }

}
