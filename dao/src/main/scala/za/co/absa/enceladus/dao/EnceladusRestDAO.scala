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

import com.typesafe.config.ConfigFactory

import org.apache.commons.httpclient.HttpStatus
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{DataType, StructType}

import za.co.absa.enceladus.model._
import za.co.absa.enceladus.model.api.versionedModelDetail.{DatasetDetail, MappingTableDetail, SchemaDetail}

import scala.util.control.NonFatal
import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.databind.SerializationFeature

object EnceladusRestDAO extends EnceladusDAO {
  val conf = ConfigFactory.load()
  val restBase = conf.getString("menas.rest.uri")
  private val userName = LoggedInUserInfo.getUserName

  private val log = LogManager.getLogger("enceladus.conformance.EnceladusRestDAO")

  val objectMapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .registerModule(new JavaTimeModule())
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  
  override def getDataset(name: String, version: Int): Dataset = {
    val url = s"$restBase/dataset/${getEncodedName(name)}/$version?uname=$userName"
    log.info(url)
    val json = authorizeGetRequest(url)
    log.info(json)
    objectMapper.readValue(json, classOf[DatasetDetail]).model
  }

  override def getMappingTable(name: String, version: Int): MappingTable = {
    val url = s"$restBase/mapping_table/${getEncodedName(name)}/$version?uname=$userName"
    log.info(url)
    val json = authorizeGetRequest(url)
    log.info(json)
    objectMapper.readValue(json, classOf[MappingTableDetail]).model
  }

  private def getSchemaByFormat(name: String, version: Int, format: String): SchemaDetail = {
    val url = s"$restBase/schema/${getEncodedName(name)}/$version?format=$format"
    val json = Source.fromURL (url).mkString
    log.info (url)
    log.info (json)
    objectMapper.readValue(json, classOf[SchemaDetail])
 }

  override def getSchema(name: String, version: Int): StructType = {
    val schema = getSchemaByFormat(name, version, "structtype")
    DataType.fromJson(schema.schemaFormatted.get).asInstanceOf[StructType]
  }

  /* The URLEncoder implements the HTML Specifications
   * so have to replace '+' with %20
   * https://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
   */
  private def getEncodedName(name: String): String ={
    java.net.URLEncoder.encode(name, "UTF-8").replace("+", "%20")
  }

  private def authorizeGetRequest(url: String): String = {
    try {
      log.info(s"URL: $url GET")
      val httpClient = HttpClients.createDefault
      val httpGet = new HttpGet(url)
      val response: CloseableHttpResponse = httpClient.execute(httpGet)

      try {
        val status = response.getStatusLine.getStatusCode
        val ok = status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES
        val unAuthorized = status == HttpStatus.SC_UNAUTHORIZED
        val content = {
          if (ok) {
            readResponseObject(response)
          }
          else if (unAuthorized) {
            throw new UnauthorizedException
          }
          else {
            throw new DaoException(s"Server returned HTTP response code: $status for Menas URL:  $url ")
          }
        }
        content.getOrElse("")
      }
      finally {
        response.close()
      }
    }
    catch {
      case unAuthException: UnauthorizedException => throw unAuthException
      case daoException: DaoException => throw daoException
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
