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

import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.LoggerFactory
import org.springframework.http._
import org.springframework.web.client.RestTemplate
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.model.{Dataset, MappingTable, Run, SplineReference}

import scala.reflect.ClassTag

/**
  * Implementation of Menas REST API DAO
  */
protected class MenasRestDAO(private[dao] val apiBaseUrl: String,
                             private[dao] val authClient: AuthClient,
                             private[dao] val restTemplate: RestTemplate) extends MenasDAO {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val runsUrl = s"$apiBaseUrl/runs"

  private[dao] var authHeaders: HttpHeaders = new HttpHeaders()

  def authenticate(): Unit = {
    authHeaders = authClient.authenticate()
  }

  def getDataset(name: String, version: Int): Dataset = {
    val url = s"$apiBaseUrl/dataset/detail/${encode(name)}/$version"
    sendGet[Dataset](url)
  }

  def getMappingTable(name: String, version: Int): MappingTable = {
    val url = s"$apiBaseUrl/mappingTable/detail/${encode(name)}/$version"
    sendGet[MappingTable](url)
  }

  def getSchema(name: String, version: Int): StructType = {
    val url = s"$apiBaseUrl/schema/json/${encode(name)}/$version"
    val json = sendGet[String](url)
    DataType.fromJson(json).asInstanceOf[StructType]
  }

  def getSchemaAttachment(name: String, version: Int): String = {
    val url = s"$apiBaseUrl/schema/export/${encode(name)}/$version"
    sendGet[String](url)
  }

  def storeNewRunObject(run: Run): String = {
    sendPost[Run, Run](runsUrl, bodyOpt = Option(run)).uniqueId.get
  }

  def updateControlMeasure(uniqueId: String,
                           controlMeasure: ControlMeasure): Boolean = {
    val url = s"$runsUrl/updateControlMeasure/$uniqueId"

    sendPost[ControlMeasure, String](url, bodyOpt = Option(controlMeasure))
    true
  }

  def updateRunStatus(uniqueId: String,
                      runStatus: RunStatus): Boolean = {
    val url = s"$runsUrl/updateRunStatus/$uniqueId"

    sendPost[RunStatus, String](url, bodyOpt = Option(runStatus))
    true
  }

  def updateSplineReference(uniqueId: String,
                            splineRef: SplineReference): Boolean = {
    val url = s"$runsUrl/updateSplineReference/$uniqueId"

    sendPost[SplineReference, String](url, bodyOpt = Option(splineRef))
    true
  }

  def appendCheckpointMeasure(uniqueId: String,
                              checkpoint: Checkpoint): Boolean = {
    val url = s"$runsUrl/addCheckpoint/$uniqueId"

    sendPost[Checkpoint, String](url, bodyOpt = Option(checkpoint))
    true
  }

  /* The URLEncoder implements the HTML Specifications
   * so have to replace '+' with %20
   * https://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
   */
  private def encode(string: String): String = {
    java.net.URLEncoder.encode(string, "UTF-8").replace("+", "%20")
  }

  private def sendGet[T](urlPath: String,
                         headers: HttpHeaders = new HttpHeaders())
                        (implicit ct: ClassTag[T]): T = {
    send[T, T](HttpMethod.GET, urlPath, headers)
  }

  private def sendPost[B, T](urlPath: String,
                             headers: HttpHeaders = new HttpHeaders(),
                             bodyOpt: Option[B] = None)
                            (implicit ct: ClassTag[T]): T = {
    headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
    send[B, T](HttpMethod.POST, urlPath, headers, bodyOpt)
  }

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
      case None       => new HttpEntity[B](headers)
    }

    val response = restTemplate.exchange(url, method, httpEntity, classOf[String])

    val statusCode = response.getStatusCode

    statusCode match {
      case HttpStatus.OK | HttpStatus.CREATED             =>
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

      case HttpStatus.NOT_FOUND                           =>
        throw DaoException(s"Entity not found - $statusCode")

      case _                                              =>
        throw DaoException(s"Response - $statusCode : ${Option(response.getBody).getOrElse("None")}")
    }
  }

}
