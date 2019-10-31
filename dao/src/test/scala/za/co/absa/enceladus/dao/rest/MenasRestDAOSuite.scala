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

package za.co.absa.enceladus.dao.rest

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.mockito.Mockito
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.test.VersionedModelMatchers
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory, RunFactory}
import za.co.absa.enceladus.model.{Dataset, MappingTable, Run, SplineReference}

class MenasRestDAOSuite extends BaseTestSuite with VersionedModelMatchers {

  private val name = "name"
  private val version = 1
  private val uniqueId = "503a5b7f-2171-41bd-9a46-91e1135dcb01"
  private val schemaJson =
    """{
      |  "type": "struct",
      |  "fields": [
      |    {
      |      "name": "age",
      |      "type": "long",
      |      "nullable": true,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "name",
      |      "type": "string",
      |      "nullable": false,
      |      "metadata": {}
      |    }
      |  ]
      |}
    """.stripMargin

  private val apiBaseUrl = "http://test1"
  private val apiCaller = new ApiCallerStub(apiBaseUrl)
  private val restClient = mock[RestClient]

  private val menasDao: MenasDAO = new MenasRestDAO(apiCaller, restClient)

  before {
    Mockito.reset(restClient)
  }

  "MenasRestDAO" can {
    "authenticate by calling AuthClient::authenticate" in {
      menasDao.authenticate()

      Mockito.verify(restClient, Mockito.only()).authenticate()
    }

    "getDataset" in {
      val expected = DatasetFactory.getDummyDataset(name, version)
      val url = s"$apiBaseUrl/api/dataset/detail/$name/$version"
      Mockito.when(restClient.sendGet[Dataset](url)).thenReturn(expected)

      val result = menasDao.getDataset(name, version)

      result should matchTo(expected)
    }

    "getMappingTable" in {
      val expected = MappingTableFactory.getDummyMappingTable(name, version)
      val url = s"$apiBaseUrl/api/mappingTable/detail/$name/$version"
      Mockito.when(restClient.sendGet[MappingTable](url)).thenReturn(expected)

      val result = menasDao.getMappingTable(name, version)

      result should matchTo(expected)
    }

    "getSchema" in {
      val expected = StructType(Seq(
        StructField(
          name = "age",
          dataType = LongType,
          nullable = true),
        StructField(
          name = "name",
          dataType = StringType,
          nullable = false)
      ))

      Mockito.when(restClient.sendGet[String](s"$apiBaseUrl/api/schema/json/$name/$version")).thenReturn(schemaJson)

      val result = menasDao.getSchema(name, version)

      result should be(expected)
    }

    "getSchemaAttachment" in {
      Mockito.when(restClient.sendGet[String](s"$apiBaseUrl/api/schema/export/$name/$version")).thenReturn(schemaJson)

      val result = menasDao.getSchemaAttachment(name, version)

      result should be(schemaJson)
    }

    "storeNewRunObject" in {
      val requestRun = RunFactory.getDummyRun(uniqueId = None)
      val responseRun = requestRun.copy(uniqueId = Some(uniqueId))
      val url = s"$apiBaseUrl/api/runs"
      Mockito.when(restClient.sendPost[Run, Run](url, requestRun)).thenReturn(responseRun)

      val result = menasDao.storeNewRunObject(requestRun)

      result should be(responseRun)
      Mockito.verify(restClient, Mockito.only()).sendPost[Run, Run](url, requestRun)
    }

    "updateControlMeasure" in {
      val controlMeasure = RunFactory.getDummyControlMeasure()
      val responseRun = RunFactory.getDummyRun(uniqueId = Some(uniqueId), controlMeasure = controlMeasure)
      val url = s"$apiBaseUrl/api/runs/updateControlMeasure/$uniqueId"
      Mockito.when(restClient.sendPost[ControlMeasure, Run](url, controlMeasure)).thenReturn(responseRun)

      val result = menasDao.updateControlMeasure(uniqueId, controlMeasure)

      result should be(responseRun)
      Mockito.verify(restClient, Mockito.only()).sendPost[ControlMeasure, Run](url, controlMeasure)
    }

    "updateRunStatus" in {
      val runStatus = RunFactory.getDummyRunStatus()
      val responseRun = RunFactory.getDummyRun(uniqueId = Some(uniqueId), runStatus = runStatus)
      val url = s"$apiBaseUrl/api/runs/updateRunStatus/$uniqueId"
      Mockito.when(restClient.sendPost[RunStatus, Run](url, runStatus)).thenReturn(responseRun)

      val result = menasDao.updateRunStatus(uniqueId, runStatus)

      result should be(responseRun)
      Mockito.verify(restClient, Mockito.only()).sendPost[RunStatus, Run](url, runStatus)
    }

    "updateSplineReference" in {
      val splineReference = RunFactory.getDummySplineReference()
      val responseRun = RunFactory.getDummyRun(uniqueId = Some(uniqueId), splineRef = splineReference)
      val url = s"$apiBaseUrl/api/runs/updateSplineReference/$uniqueId"
      Mockito.when(restClient.sendPost[SplineReference, Run](url, splineReference)).thenReturn(responseRun)

      val result = menasDao.updateSplineReference(uniqueId, splineReference)

      result should be(responseRun)
      Mockito.verify(restClient, Mockito.only()).sendPost[SplineReference, Run](url, splineReference)
    }

    "appendCheckpointMeasure" in {
      val checkpoint = RunFactory.getDummyCheckpoint()
      val controlMeasure = RunFactory.getDummyControlMeasure(checkpoints = List(checkpoint))
      val responseRun = RunFactory.getDummyRun(uniqueId = Some(uniqueId), controlMeasure = controlMeasure)
      val url = s"$apiBaseUrl/api/runs/addCheckpoint/$uniqueId"
      Mockito.when(restClient.sendPost[Checkpoint, Run](url, checkpoint)).thenReturn(responseRun)

      val result = menasDao.appendCheckpointMeasure(uniqueId, checkpoint)

      result should be(responseRun)
      Mockito.verify(restClient, Mockito.only()).sendPost[Checkpoint, Run](url, checkpoint)
    }
  }

}
