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

import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset, MappingTable, Run, SplineReference, Validation}

/**
  * Implementation of Menas REST API DAO
  */
protected class MenasRestDAO(private[rest] val apiCaller: ApiCaller,
                             private[rest] val restClient: RestClient) extends MenasDAO {

  def authenticate(): Unit = {
    restClient.authenticate()
  }

  def getDataset(name: String, version: Int, validateProperties: Boolean = false): Dataset = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/dataset/$name/$version?validateProperties=$validateProperties"
      restClient.sendGet[Dataset](url)
    }
  }

  def getDatasetPropertiesForInfoFile(datasetName: String, datasetVersion: Int): Map[String, String] = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/dataset/$datasetName/$datasetVersion/properties/?putIntoInfoFile=true"
      restClient.sendGet[Map[String, String]](url)
    }
  }

  def getMappingTable(name: String, version: Int): MappingTable = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/mappingTable/detail/$name/$version"
      restClient.sendGet[MappingTable](url)
    }
  }

  def getSchema(name: String, version: Int): StructType = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/schema/json/$name/$version"
      val json = restClient.sendGet[String](url)
      DataType.fromJson(json).asInstanceOf[StructType]
    }
  }

  def getSchemaAttachment(name: String, version: Int): String = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/schema/export/$name/$version"
      restClient.sendGet[String](url)
    }
  }

  def storeNewRunObject(run: Run): Run = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/runs"
      restClient.sendPost[Run, Run](url, run)
    }
  }

  def updateControlMeasure(uniqueId: String,
                           controlMeasure: ControlMeasure): Run = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/runs/updateControlMeasure/$uniqueId"
      restClient.sendPost[ControlMeasure, Run](url, controlMeasure)
    }
  }

  def updateRunStatus(uniqueId: String,
                      runStatus: RunStatus): Run = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/runs/updateRunStatus/$uniqueId"
      restClient.sendPost[RunStatus, Run](url, runStatus)
    }
  }

  def updateSplineReference(uniqueId: String,
                            splineRef: SplineReference): Run = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/runs/updateSplineReference/$uniqueId"
      restClient.sendPost[SplineReference, Run](url, splineRef)
    }
  }

  def appendCheckpointMeasure(uniqueId: String,
                              checkpoint: Checkpoint): Run = {
    apiCaller.call { apiBaseUrl =>
      val url = s"$apiBaseUrl/api/runs/addCheckpoint/$uniqueId"
      restClient.sendPost[Checkpoint, Run](url, checkpoint)
    }
  }

}
