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
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.model.{Dataset, MappingTable, Run, SplineReference}

/**
  * Implementation of Menas REST API DAO
  */
protected class MenasRestDAO(private[dao] val apiBaseUrl: String,
                             private[dao] val restClient: RestClient) extends MenasDAO {

  private val runsUrl = s"$apiBaseUrl/runs"

  def authenticate(): Unit = {
    restClient.authenticate()
  }

  def getDataset(name: String, version: Int): Dataset = {
    val url = s"$apiBaseUrl/dataset/detail/${encode(name)}/$version"
    restClient.sendGet[Dataset](url)
  }

  def getMappingTable(name: String, version: Int): MappingTable = {
    val url = s"$apiBaseUrl/mappingTable/detail/${encode(name)}/$version"
    restClient.sendGet[MappingTable](url)
  }

  def getSchema(name: String, version: Int): StructType = {
    val url = s"$apiBaseUrl/schema/json/${encode(name)}/$version"
    val json = restClient.sendGet[String](url)
    DataType.fromJson(json).asInstanceOf[StructType]
  }

  def getSchemaAttachment(name: String, version: Int): String = {
    val url = s"$apiBaseUrl/schema/export/${encode(name)}/$version"
    restClient.sendGet[String](url)
  }

  def storeNewRunObject(run: Run): String = {
    restClient.sendPost[Run, Run](runsUrl, run).uniqueId.get
  }

  def updateControlMeasure(uniqueId: String,
                           controlMeasure: ControlMeasure): Boolean = {
    val url = s"$runsUrl/updateControlMeasure/$uniqueId"

    restClient.sendPost[ControlMeasure, String](url, controlMeasure)
    true
  }

  def updateRunStatus(uniqueId: String,
                      runStatus: RunStatus): Boolean = {
    val url = s"$runsUrl/updateRunStatus/$uniqueId"

    restClient.sendPost[RunStatus, String](url, runStatus)
    true
  }

  def updateSplineReference(uniqueId: String,
                            splineRef: SplineReference): Boolean = {
    val url = s"$runsUrl/updateSplineReference/$uniqueId"

    restClient.sendPost[SplineReference, String](url, splineRef)
    true
  }

  def appendCheckpointMeasure(uniqueId: String,
                              checkpoint: Checkpoint): Boolean = {
    val url = s"$runsUrl/addCheckpoint/$uniqueId"

    restClient.sendPost[Checkpoint, String](url, checkpoint)
    true
  }

  /* The URLEncoder implements the HTML Specifications
   * so have to replace '+' with %20
   * https://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
   */
  private def encode(string: String): String = {
    java.net.URLEncoder.encode(string, "UTF-8").replace("+", "%20")
  }

}
