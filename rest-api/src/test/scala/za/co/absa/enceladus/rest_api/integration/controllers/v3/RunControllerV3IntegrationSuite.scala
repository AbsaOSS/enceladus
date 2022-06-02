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

package za.co.absa.enceladus.rest_api.integration.controllers.v3

import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunError, RunState, RunStatus}
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, RunFactory}
import za.co.absa.enceladus.model.{Run, SplineReference, Validation}
import za.co.absa.enceladus.rest_api.integration.controllers.BaseRestApiTestV3
import za.co.absa.enceladus.rest_api.integration.fixtures.{DatasetFixtureService, FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary}

import java.util.UUID

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class RunControllerV3IntegrationSuite extends BaseRestApiTestV3 with Matchers {

  import za.co.absa.enceladus.rest_api.integration.RunImplicits.RunExtensions

  @Autowired
  private val runFixture: RunFixtureService = null

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  override def fixtures: List[FixtureService[_]] = List(runFixture, datasetFixture)

  private val apiUrl = "/runs"

  s"GET $apiUrl" can {
    "return 200" when {
      "latest RunSummaries are queried" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
        runFixture.add(dataset1ver1run1, dataset1ver1run2)
        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
        runFixture.add(dataset1ver2run1)
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
        runFixture.add(dataset2ver1run1)

        val response = sendGet[String](s"$apiUrl")

        assertOk(response)

        val actual = response.getBody
        val expected = SerializationUtils.asJson(Seq(dataset1ver1run2, dataset1ver2run1, dataset2ver1run1).map(_.toSummary))
        actual shouldBe expected
      }

      "latest RunSummaries are queried on startDate" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "18-05-2022 13:01:12 +0200")
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "22-05-2022 14:01:12 +0200")

        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "19-05-2022 15:01:12 +0200")
        val dataset1ver2run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 2, startDateTime = "22-05-2022 15:01:12 +0200")
        val dataset1ver2run3 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 3, startDateTime = "23-05-2022 15:01:12 +0200")

        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "17-05-2022 13:01:12 +0200")
        val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "20-05-2022 13:01:12 +0200")
        runFixture.add(
          dataset1ver1run1, dataset1ver1run2,
          dataset1ver2run1, dataset1ver2run2, dataset1ver2run3,
          dataset2ver1run1, dataset3ver1run1
        )

        val response = sendGet[Array[RunSummary]](s"$apiUrl?startDate=20-05-2022")
        val expected = Array(dataset1ver1run2, dataset1ver2run3, dataset3ver1run1).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "latest RunSummaries are queried on uniqueId" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, uniqueId = Some("12345678-90ab-cdef-1234-567890abcdef"))
        val run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, uniqueId = Some(UUID.randomUUID().toString)) // some other id
        val run3 = RunFactory.getDummyRun(dataset = "datasetX", uniqueId = None)
        runFixture.add(run1, run2, run3)

        val response = sendGet[Array[RunSummary]](s"$apiUrl?uniqueId=12345678-90ab-cdef-1234-567890abcdef")
        val expected = Array(run1).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "latest RunSummaries are queried on sparkAppId reference" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1",
          controlMeasure = RunFactory.getDummyControlMeasure(
            metadata = RunFactory.getDummyMetadata(additionalInfo = Map(
              "std_application_id" -> "application_1653565036000_00001"
            ))
          )
        )

        val run2 = RunFactory.getDummyRun(dataset = "dataset2",
          controlMeasure = RunFactory.getDummyControlMeasure(
            metadata = RunFactory.getDummyMetadata(additionalInfo = Map(
              "conform_application_id" -> "application_1653565036000_00002"
            ))
          )
        )
        runFixture.add(run1, run2)

        // get summary of run1 by std app_id
        val response = sendGet[Array[RunSummary]](s"$apiUrl?sparkAppId=application_1653565036000_00001")
        response.getBody shouldBe Seq(run1).map(_.toSummary)

        // get summary of run2 by conform app_id
        val response2 = sendGet[Array[RunSummary]](s"$apiUrl?sparkAppId=application_1653565036000_00002")
        response2.getBody shouldBe Seq(run2).map(_.toSummary)
      }
      "latest RunSummaries are queried, but nothing is found" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", startDateTime = "22-05-2022 14:01:12 +0200")
        val run2 = RunFactory.getDummyRun(dataset = "dataset3", uniqueId = None)
        runFixture.add(run1, run2)

        val response = sendGet[String](s"$apiUrl?startDate=24-05-2022")
        response.getBody shouldBe "[]" // empty array
      }
    }

    "return 400" when {
      "queried on mutually exclusive options" in {
        val response = sendGet[String](s"$apiUrl?uniqueId=12345678-90ab-cdef-1234-567890abcdef&sparkAppId=appId1")
        response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
        response.getBody should include("You may only supply one of [startDate|sparkAppId|uniqueId]")

        val response2 = sendGet[String](s"$apiUrl?startDate=20-02-2022&sparkAppId=appId1")
        response2.getStatusCode shouldBe HttpStatus.BAD_REQUEST
        response2.getBody should include("You may only supply one of [startDate|sparkAppId|uniqueId]")
      }
    }
  }

  s"GET $apiUrl/{datasetName}" can {
    "return 200" when {
      "latest RunSummaries are queried" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
        runFixture.add(dataset1ver1run1, dataset1ver1run2)
        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
        runFixture.add(dataset1ver2run1)
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
        runFixture.add(dataset2ver1run1) // unrelated to dataset1

        val response = sendGet[String](s"$apiUrl/dataset1")
        assertOk(response)

        val actual = response.getBody
        val expected = SerializationUtils.asJson(Seq(dataset1ver1run2, dataset1ver2run1).map(_.toSummary))
        actual shouldBe expected
      }

      "latest RunSummaries are queried on startDate" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "18-05-2022 13:01:12 +0200")
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "22-05-2022 14:01:12 +0200")

        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "19-05-2022 15:01:12 +0200")
        val dataset1ver2run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 2, startDateTime = "22-05-2022 15:01:12 +0200")
        val dataset1ver2run3 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 3, startDateTime = "23-05-2022 15:01:12 +0200")

        // unrelated to dataset1:
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "17-05-2022 13:01:12 +0200")
        val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "20-05-2022 13:01:12 +0200")
        runFixture.add(
          dataset1ver1run1, dataset1ver1run2,
          dataset1ver2run1, dataset1ver2run2, dataset1ver2run3,
          dataset2ver1run1, dataset3ver1run1
        )

        val response = sendGet[Array[RunSummary]](s"$apiUrl/dataset1?startDate=20-05-2022")
        response.getStatusCode shouldBe HttpStatus.OK
        val expected = Array(dataset1ver1run2, dataset1ver2run3).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "latest RunSummaries are queried on uniqueId" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, uniqueId = Some("12345678-90ab-cdef-1234-567890abcdef"))
        val run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, uniqueId = Some(UUID.randomUUID().toString)) // some other id
        val run3 = RunFactory.getDummyRun(dataset = "dataset1", runId = 3, uniqueId = None)
        val run4 = RunFactory.getDummyRun(dataset = "dataset2", uniqueId = None) // unrelated to dataset1
        runFixture.add(run1, run2, run3, run4)

        val response = sendGet[Array[RunSummary]](s"$apiUrl?uniqueId=12345678-90ab-cdef-1234-567890abcdef")
        response.getStatusCode shouldBe HttpStatus.OK
        val expected = Array(run1).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "latest RunSummaries are queried, but nothing is found" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", startDateTime = "22-05-2022 14:01:12 +0200")
        val run2 = RunFactory.getDummyRun(dataset = "dataset3", uniqueId = None) // unrelated to dataset1
        runFixture.add(run1, run2)

        val response = sendGet[String](s"$apiUrl/dataset1?startDate=24-05-2022")
        response.getStatusCode shouldBe HttpStatus.OK
        response.getBody shouldBe "[]" // empty array
      }
    }

  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}" can {
    "return 200" when {
      "return RunSummaries by dataset name and version" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
        runFixture.add(dataset1ver1run1, dataset1ver1run2)
        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
        runFixture.add(dataset1ver2run1, dataset2ver1run1)

        val response = sendGet[Array[RunSummary]](s"$apiUrl/dataset1/1")
        response.getStatusCode shouldBe HttpStatus.OK

        val expected = List(dataset1ver1run1, dataset1ver1run2).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "return RunSummaries on combination of (startDate, dsName, and dsVersion)" in {
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "22-05-2022 14:01:12 +0200")

        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "19-05-2022 15:01:12 +0200")
        val dataset1ver2run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 2, startDateTime = "20-05-2022 15:01:12 +0200")
        val dataset1ver2run3 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 3, startDateTime = "23-05-2022 15:01:12 +0200")

        val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "21-05-2022 13:01:12 +0200")

        runFixture.add(
          dataset1ver1run2,
          dataset1ver2run1, dataset1ver2run2, dataset1ver2run3,
          dataset3ver1run1
        )

        val response = sendGet[Array[RunSummary]](s"$apiUrl/dataset1/2?startDate=20-05-2022")
        response.getStatusCode shouldBe HttpStatus.OK

        val expected = List(dataset1ver2run2, dataset1ver2run3).map(_.toSummary)
        response.getBody shouldBe expected

      }
    }
  }

  s"POST $apiUrl/{datasetName}/{datasetVersion}" can {
    "return 201" when {
      "new Run is created (basic case)" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset")) // dataset ref'd by the run
        val run = RunFactory.getDummyRun()

        val response = sendPost[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))
        assertCreated(response)

        val body = response.getBody
        assert(body == run)
      }
      "created run provides a uniqueId if none is specified" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run = RunFactory.getDummyRun(uniqueId = None)
        val response = sendPost[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))

        assertCreated(response)

        val body = response.getBody
        assert(body.uniqueId.isDefined)
        val expected = run.copy(uniqueId = body.uniqueId)
        assert(body == expected)
      }
      "created run generates a runId=1 for the first run" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run = RunFactory.getDummyRun(runId = 123) // specified runId is ignored

        val response = sendPost[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))

        assertCreated(response)

        val body = response.getBody
        assert(body == run.copy(runId = 1)) // no runs present, so runId = 1
      }
      "created run generates a subsequent runId" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        runFixture.add(
          RunFactory.getDummyRun(runId = 1),
          RunFactory.getDummyRun(runId = 2)
        )
        val run = RunFactory.getDummyRun(runId = 222) // specified runId is ignored, subsequent is used

        val response = sendPost[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))

        assertCreated(response)

        val body = response.getBody
        assert(body == run.copy(runId = 3)) // runs 1, and 2 were already presents
      }
      "handles two runs being started simultaneously" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run1 = RunFactory.getDummyRun()
        val run2 = RunFactory.getDummyRun()
        run1.uniqueId should not be run2.uniqueId

        val eventualResponse1 = sendPostAsync[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run1))
        val eventualResponse2 = sendPostAsync[Run, Run](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run2))

        val response1 = await(eventualResponse1)
        val response2 = await(eventualResponse2)

        assertCreated(response1)
        assertCreated(response2)

        val body1 = response1.getBody
        val body2 = response2.getBody
        body1 shouldBe body1.copy(runId = body1.runId) // still the same run object, just different runId
        body2 shouldBe body2.copy(runId = body2.runId)

        Set(body1.runId, body2.runId) shouldBe Set(1, 2)
      }
    }

    "return 400" when {
      "sending incorrect run data" in {
        val response = sendPost[String, String](s"$apiUrl/dummyDataset/1", bodyOpt = Some("{}"))

        assertBadRequest(response)

        val body = response.getBody
        body should include("URL and payload entity name mismatch: 'dummyDataset' != 'null'")
      }
      "sending mismatched URL/payload data" in {
        val response1 = sendPost[Run, String](s"$apiUrl/datasetB/2",
          bodyOpt = Some(RunFactory.getDummyRun(dataset = "datasetA", datasetVersion = 2)))
        assertBadRequest(response1)
        response1.getBody should include("URL and payload entity name mismatch: 'datasetB' != 'datasetA'")

        val response2 = sendPost[Run, String](s"$apiUrl/datasetC/2",
          bodyOpt = Some(RunFactory.getDummyRun(dataset = "datasetC", datasetVersion = 3)))
        assertBadRequest(response2)
        response2.getBody should include("URL and payload entity version mismatch: 2 != 3")
      }
      "a Run with the given uniqueId already exists" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"
        val presentRun = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
        runFixture.add(presentRun)
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))

        val response = sendPost[Run, Validation](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))

        assertBadRequest(response)

        val body = response.getBody
        assert(!body.isValid)
        assert(body == Validation().withError("uniqueId", s"run with this uniqueId already exists: $uniqueId"))
      }
      "a Run references a non-existing dataset" in {
        // dataset ref'd by the run does not exits
        val run = RunFactory.getDummyRun()

        val response = sendPost[Run, Validation](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))
        response.getStatusCode shouldBe HttpStatus.BAD_REQUEST

        response.getBody shouldBe Validation.empty.withError("dataset", "Dataset dummyDataset v1 not found!")
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/{runId}" can {
    "return 200" when {
      "return Run by dataset name, version, and runId" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
        runFixture.add(dataset1ver1run1, dataset1ver1run2)

        val response = sendGet[String](s"$apiUrl/dataset1/1/2")
        response.getStatusCode shouldBe HttpStatus.OK

        response.getBody shouldBe
          s"""{
             |"uniqueId":"${dataset1ver1run2.uniqueId.get}",
             |"runId":2,
             |"dataset":"dataset1",
             |"datasetVersion":1,
             |"splineRef":{"sparkApplicationId":"dummySparkApplicationId","outputPath":"dummyOutputPath"},
             |"startDateTime":"${dataset1ver1run2.startDateTime}",
             |"runStatus":{"status":"allSucceeded","error":null},
             |"controlMeasure":{
             |"metadata":{
             |"sourceApplication":"dummySourceApplication",
             |"country":"dummyCountry",
             |"historyType":"dummyHistoryType",
             |"dataFilename":"dummyDataFilename",
             |"sourceType":"dummySourceType",
             |"version":1,
             |"informationDate":"04-12-2017 16:19:17 +0200",
             |"additionalInfo":{}
             |},
             |"runUniqueId":"${dataset1ver1run2.controlMeasure.runUniqueId.get}",
             |"checkpoints":[]
             |}
             |}""".stripMargin.replaceAll("\n", "")
      }
    }
    "return 400" when {
      "run does not exists" in {
        val response = sendGet[String](s"$apiUrl/dataset1/1/2")
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"PUT $apiUrl/{datasetName}/{datasetVersion}/{runId}" can {
    "return 200" when {
      "run is updated (running -> allSucceeded)" in {
        val run = RunFactory.getDummyRun(runStatus = RunStatus(RunState.running, None))
        runFixture.add(run)

        val response = sendPut[RunStatus, Run](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(RunStatus(RunState.allSucceeded, None)))
        assertOk(response)

        val body = response.getBody
        assert(body == run.copy(runStatus = RunStatus(RunState.allSucceeded, None)))
      }
      "run is updated (running -> failed with error)" in {
        val run = RunFactory.getDummyRun(runStatus = RunStatus(RunState.running, None))
        runFixture.add(run)

        val runError = RunError("job1", "step2", "desc3", "details4")
        val newRunStatus = RunStatus(RunState.failed, Some(runError))
        val response = sendPut[RunStatus, Run](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(newRunStatus))
        assertOk(response)

        val body = response.getBody
        body.runStatus shouldBe newRunStatus
      }
    }

    "return 400" when {
      "sending incorrect run data" in {
        val run = RunFactory.getDummyRun(runStatus = RunStatus(RunState.running, None))
        runFixture.add(run)
        val response = sendPut[String, String](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Some("""{"badField":123}"""))
        assertBadRequest(response)

        val body = response.getBody
        body should include("Invalid empty RunStatus submitted")
      }

      "a RunState-update references a non-existing run" in {
        // Run ref'd by the run does not exits
        val response = sendPut[RunStatus, Validation](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(RunStatus(RunState.allSucceeded, None)))
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  // todo add other endpoints test cases
}
