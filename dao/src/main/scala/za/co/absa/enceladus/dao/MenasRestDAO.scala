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

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.log4j.LogManager
import za.co.absa.atum.model._
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.dao.EnceladusRestDAO.{csrfToken, sessionCookie, userName}
import za.co.absa.enceladus.model._

import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal
import org.apache.http.HttpStatus

/** Implements routines for Menas REST API. */
object MenasRestDAO extends MenasDAO {
  private val conf = ConfigFactory.load()
  private val restBase = conf.getString("menas.rest.uri")
  private val log = LogManager.getLogger("ControlFrameworkREST")

  /**
    * Stores a new Run object in the database by sending REST request to Menas
    *
    * @param run A Run object
    * @return The unique id of newly created Run object or encapsulated exception
    */
  def storeNewRunObject(run: Run): Try[String] = {
    Try({
      val runToSave = if (run.uniqueId.isEmpty) run.copy(uniqueId = Some(UUID.randomUUID().toString)) else run
      val json = EnceladusRestDAO.objectMapper.writeValueAsString(runToSave)
      val url = s"$restBase/runs"

      if (sendPostJson(url, json)) {
        runToSave.uniqueId.get
      }
      else {
        throw new IllegalStateException("Unable to store a Control Framework Run object in the database.")
      }
    })
  }

  /**
    * Updates control measure object of the specified run
    *
    * @param uniqueId       An unique id of a Run object
    * @param controlMeasure Control Measures
    * @return true if Run object is successfully updated
    */
  def updateControlMeasure(uniqueId: String,
                           controlMeasure: ControlMeasure): Boolean = {
    val url = s"$restBase/runs/updateControlMeasure/$uniqueId"
    val json = EnceladusRestDAO.objectMapper.writeValueAsString(controlMeasure)

    sendPostJson(url, json)
  }

  def updateRunStatus(uniqueId: String,
                      runStatus: RunStatus): Boolean = {
    val url = s"$restBase/runs/updateRunStatus/$uniqueId"
    val json = EnceladusRestDAO.objectMapper.writeValueAsString(runStatus)

    sendPostJson(url, json)
  }

  /**
    * Updates spline reference of the specified run
    *
    * @param uniqueId  An unique id of a Run object
    * @param splineRef Spline Reference
    * @return true if Run object is successfully updated
    */
  def updateSplineReference(uniqueId: String,
                            splineRef: SplineReference): Boolean = {
    val url = s"$restBase/runs/updateSplineReference/$uniqueId"
    val json = EnceladusRestDAO.objectMapper.writeValueAsString(splineRef)

    sendPostJson(url, json)
  }

  /**
    * Creates new Run object in the database by loading control measurements from
    * _INFO file accompanied by output data
    *
    * @param uniqueId   An unique id of a Run object
    * @param checkpoint A checkpoint to be appended to the database
    * @return true if Run object is successfully updated
    */
  def appendCheckpointMeasure(uniqueId: String,
                              checkpoint: Checkpoint): Boolean = {
    val url = s"$restBase/runs/addCheckpoint/$uniqueId"
    val json = EnceladusRestDAO.objectMapper.writeValueAsString(checkpoint)

    sendPostJson(url, json)
  }

  private def sendPostJson(url: String, json: String): Boolean = {
    try {
      log.info(s"URL: $url POST: $json")
      val httpClient = HttpClients.createDefault
      val httpPost = new HttpPost(url)
      httpPost.addHeader("cookie", sessionCookie)
      httpPost.addHeader("X-CSRF-TOKEN", csrfToken)
      httpPost.addHeader("content-type", "application/json")

      httpPost.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON))

      val response: CloseableHttpResponse = httpClient.execute(httpPost)
      try {
        val status = response.getStatusLine.getStatusCode
        val ok = status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES
        val unAuthorized = status == HttpStatus.SC_UNAUTHORIZED
        if (ok) {
          log.info(response.toString)
        }
        else if (unAuthorized) {
          throw new UnauthorizedException
        }
        else {
          val responseBody = getResponseBody(response)
          log.error(s"RESPONSE: ${response.getStatusLine} - $responseBody")
        }
        ok
      }
      finally {
        response.close()
      }
    }
    catch {
      case unAuthException: UnauthorizedException => throw unAuthException
      case NonFatal(e) =>
        log.error(s"Unable to connect to Menas endpoint via $url with error: ${e.getMessage}")
        false
    }
  }

  private def getResponseBody(response: CloseableHttpResponse): String = {
    Source.fromInputStream(response.getEntity.getContent).mkString
  }

}
