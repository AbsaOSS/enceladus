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
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, RunError, RunState, RunStatus}
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.enceladus.model.test.factories.RunFactory.{getDummyMeasurement, getDummyRun}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, RunFactory}
import za.co.absa.enceladus.model.{Run, SplineReference, Validation}
import za.co.absa.enceladus.rest_api.integration.controllers.BaseRestApiTestV3
import za.co.absa.enceladus.rest_api.integration.controllers.TestPaginatedMatchers.conformTo
import za.co.absa.enceladus.rest_api.integration.fixtures.{DatasetFixtureService, FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.models.rest.Paginated
import za.co.absa.enceladus.rest_api.models.rest.MessageWrapper
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

  // scalastyle:off magic.number - the test deliberately contains test actual data (no need for DRY here)
  s"GET $apiUrl" can {
    "return 200" when {
      "latest RunSummaries are queried" in {
        val extraRuns1 = ('A' to 'B').map(suffix => RunFactory.getDummyRun(dataset = s"dataset$suffix"))
        runFixture.add(extraRuns1: _*) // filler to show offset

        val datasetCver1run1 = RunFactory.getDummyRun(dataset = "datasetC", datasetVersion = 1, runId = 1)
        val datasetCver1run2 = RunFactory.getDummyRun(dataset = "datasetC", datasetVersion = 1, runId = 2)
        runFixture.add(datasetCver1run1, datasetCver1run2)
        val datasetCver2run1 = RunFactory.getDummyRun(dataset = "datasetC", datasetVersion = 2, runId = 1)
        runFixture.add(datasetCver2run1)
        val datasetDver1run1 = RunFactory.getDummyRun(dataset = "datasetD", datasetVersion = 1, runId = 1)
        runFixture.add(datasetDver1run1)
        val extraRuns2 = ('E' to 'H').map(suffix => RunFactory.getDummyRun(dataset = s"dataset$suffix"))
        runFixture.add(extraRuns2: _*) // filler to show limit

        val response = sendGet[String](s"$apiUrl?offset=2&limit=3")
        assertOk(response)

        val actual = response.getBody
        val expected = SerializationUtils.asJson(
          Paginated(Seq(datasetCver1run2, datasetCver2run1, datasetDver1run1).map(_.toSummary),
            offset = 2, limit = 3, truncated = true)
        )
        actual shouldBe expected
      }

      "latest RunSummaries are queried on startDate" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "30-01-2000 13:01:12 +0200")
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "22-05-2022 14:01:12 +0200")

        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "19-05-2022 15:01:12 +0200")
        val dataset1ver2run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 2, startDateTime = "22-05-2022 15:01:12 +0200")
        val dataset1ver2run3 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 3, startDateTime = "23-05-2022 15:01:12 +0200")

        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "17-05-2022 13:01:12 +0200")
        val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "01-06-2022 13:01:12 +0200")
        runFixture.add(
          dataset1ver1run1, dataset1ver1run2,
          dataset1ver2run1, dataset1ver2run2, dataset1ver2run3,
          dataset2ver1run1, dataset3ver1run1
        )

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl?startDate=2022-05-20&limit=3")
        val expected = Paginated(Seq(dataset1ver1run2, dataset1ver2run3, dataset3ver1run1).map(_.toSummary), offset = 0,
          limit = 3, truncated = false).asTestPaginated
        response.getBody should conformTo(expected)
      }

      "latest RunSummaries are queried on uniqueId" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, uniqueId = Some("12345678-90ab-cdef-1234-567890abcdef"))
        val run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, uniqueId = Some(UUID.randomUUID().toString)) // some other id
        val run3 = RunFactory.getDummyRun(dataset = "datasetX", uniqueId = None)
        runFixture.add(run1, run2, run3)

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl?uniqueId=12345678-90ab-cdef-1234-567890abcdef")
        val expected = Paginated(Seq(run1).map(_.toSummary), offset = 0, limit = 20, truncated = false).asTestPaginated
        response.getBody should conformTo(expected)
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
        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl?sparkAppId=application_1653565036000_00001")
        response.getBody should conformTo(Paginated(Seq(run1).map(_.toSummary), offset = 0, limit = 20, truncated = false).asTestPaginated)

        // get summary of run2 by conform app_id
        val response2 = sendGet[TestPaginatedRunSummary](s"$apiUrl?sparkAppId=application_1653565036000_00002")
        response2.getBody should conformTo(Paginated(Seq(run2).map(_.toSummary), offset = 0, limit = 20, truncated = false).asTestPaginated)
      }
      "latest RunSummaries are queried, but nothing is found" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", startDateTime = "22-05-2022 14:01:12 +0200")
        val run2 = RunFactory.getDummyRun(dataset = "dataset3", uniqueId = None)
        runFixture.add(run1, run2)

        val response = sendGet[String](s"$apiUrl?startDate=2022-05-24")
        response.getBody shouldBe """{"page":[],"offset":0,"limit":20,"truncated":false}""" // empty page
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
        datasetFixture.add(
          DatasetFactory.getDummyDataset("dataset1", version = 1),
          DatasetFactory.getDummyDataset("dataset1", version = 2),
          DatasetFactory.getDummyDataset("dataset1", version = 3),
          DatasetFactory.getDummyDataset("dataset1", version = 4),
          DatasetFactory.getDummyDataset("dataset1", version = 5),
          DatasetFactory.getDummyDataset("dataset2", version = 1)
        )

        runFixture.add(
          (1 to 3).map(i => RunFactory.getDummyRun(dataset = s"dataset1", datasetVersion = i)): _*
        ) // filler to show offset

        val dataset1ver4run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 4, runId = 1)
        val dataset1ver4run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 4, runId = 2)
        runFixture.add(dataset1ver4run1, dataset1ver4run2)
        val dataset1ver5run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 5, runId = 1)
        runFixture.add(dataset1ver5run1)
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
        runFixture.add(dataset2ver1run1) // unrelated to dataset1

        runFixture.add(RunFactory.getDummyRun(dataset = s"dataset1", datasetVersion = 6)) // filler to show limit

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl/dataset1?offset=3&limit=2")
        assertOk(response)

        val actual = response.getBody
        val expected = Paginated(Seq(dataset1ver4run2, dataset1ver5run1).map(_.toSummary), offset = 3, limit=2,
          truncated = true).asTestPaginated
        actual should conformTo(expected)
      }

      "latest RunSummaries are queried on startDate" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset("dataset1", version = 1),
          DatasetFactory.getDummyDataset("dataset1", version = 2),
          DatasetFactory.getDummyDataset("dataset2", version = 1),
          DatasetFactory.getDummyDataset("dataset3", version = 1)
        )

        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "30-01-2022 13:01:12 +0200")
        val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "22-05-2022 14:01:12 +0200")

        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "19-05-2022 15:01:12 +0200")
        val dataset1ver2run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 2, startDateTime = "22-05-2022 15:01:12 +0200")
        val dataset1ver2run3 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 3, startDateTime = "01-06-2022 15:01:12 +0200")

        // unrelated to dataset1:
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "17-05-2022 13:01:12 +0200")
        val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "20-05-2022 13:01:12 +0200")
        runFixture.add(
          dataset1ver1run1, dataset1ver1run2,
          dataset1ver2run1, dataset1ver2run2, dataset1ver2run3,
          dataset2ver1run1, dataset3ver1run1
        )

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl/dataset1?startDate=2022-05-20")
        response.getStatusCode shouldBe HttpStatus.OK
        val expected = Paginated(Seq(dataset1ver1run2, dataset1ver2run3).map(_.toSummary), offset = 0, limit = 20,
          truncated = false).asTestPaginated
        response.getBody should conformTo(expected)
      }

      "latest RunSummaries are queried on uniqueId" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, uniqueId = Some("12345678-90ab-cdef-1234-567890abcdef"))
        val run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, uniqueId = Some(UUID.randomUUID().toString)) // some other id
        val run3 = RunFactory.getDummyRun(dataset = "dataset1", runId = 3, uniqueId = None)
        val run4 = RunFactory.getDummyRun(dataset = "dataset2", uniqueId = None) // unrelated to dataset1
        runFixture.add(run1, run2, run3, run4)

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl?uniqueId=12345678-90ab-cdef-1234-567890abcdef")
        response.getStatusCode shouldBe HttpStatus.OK
        val expected = Paginated(Seq(run1).map(_.toSummary), offset = 0, limit = 20, truncated = false).asTestPaginated
        response.getBody should conformTo(expected)
      }

    "latest RunSummaries are queried, but nothing is found" in {
      datasetFixture.add(
        DatasetFactory.getDummyDataset("dataset1", version = 1),
        DatasetFactory.getDummyDataset("dataset3", version = 1)
      )
      val run1 = RunFactory.getDummyRun(dataset = "dataset1", startDateTime = "22-05-2022 14:01:12 +0200")
      val run2 = RunFactory.getDummyRun(dataset = "dataset3", uniqueId = None) // unrelated to dataset1
      runFixture.add(run1, run2)

      val response = sendGet[String](s"$apiUrl/dataset1?startDate=2022-05-24")
      response.getStatusCode shouldBe HttpStatus.OK
      response.getBody shouldBe """{"page":[],"offset":0,"limit":20,"truncated":false}""" // empty page
    }
    "return RunSummaries by dataset name  - ok even for no runs for known dataset" in {
      // datasets referenced by runs must exist, too
      datasetFixture.add(
        DatasetFactory.getDummyDataset("dataset1", version = 1)
      )

      val response = sendGet[TestPaginatedRunSummary](s"$apiUrl/dataset1")
      response.getStatusCode shouldBe HttpStatus.OK

      val expected = Paginated(Seq.empty[RunSummary], offset = 0, limit = 20, truncated = false).asTestPaginated
      response.getBody should conformTo(expected)
    }
  }

  "return 404" when {
    "RunSummaries for non-existent dataset name is queried" in {
      // datasets referenced by runs must exist
      val response = sendGet[String](s"$apiUrl/dataset1")
      response.getStatusCode shouldBe HttpStatus.NOT_FOUND
    }
  }

  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}" can {
    "return 200" when {
      "return RunSummaries by dataset name and version" in {
        // datasets referenced by runs must exist, too
        datasetFixture.add(
          DatasetFactory.getDummyDataset("dataset1", version = 1),
          DatasetFactory.getDummyDataset("dataset1", version = 2),
          DatasetFactory.getDummyDataset("dataset2", version = 1)
        )

        val dataset1ver1runs = (1 to 8).map(i => RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = i))
        runFixture.add(dataset1ver1runs: _*)

        // some other runs that will not stand in the way (differ in dataset/version from the requested fitlering)
        val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
        val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
        runFixture.add(dataset1ver2run1, dataset2ver1run1)

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl/dataset1/1?offset=2&limit=3")
        response.getStatusCode shouldBe HttpStatus.OK

        val expectedRuns = Seq(dataset1ver1runs(2), dataset1ver1runs(3), dataset1ver1runs(4))
        // just to make absolutely sure:
        expectedRuns.map(_.runId) shouldBe Seq(3, 4, 5)

        val expected = Paginated(expectedRuns.map(_.toSummary), offset = 2, limit = 3, truncated = true).asTestPaginated
        response.getBody should conformTo(expected)
      }

      "return RunSummaries on combination of (startDate, dsName, and dsVersion)" in {
        // datasets referenced by runs must exist, too
        datasetFixture.add(
          DatasetFactory.getDummyDataset("dataset1", version = 1),
          DatasetFactory.getDummyDataset("dataset1", version = 2),
          DatasetFactory.getDummyDataset("dataset3", version = 1)
        )

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

        val response = sendGet[TestPaginatedRunSummary](s"$apiUrl/dataset1/2?startDate=2022-05-20")
        response.getStatusCode shouldBe HttpStatus.OK

        val expected = Paginated(Seq(dataset1ver2run2, dataset1ver2run3).map(_.toSummary), offset = 0, limit = 20,
          truncated = false).asTestPaginated
        response.getBody should conformTo(expected)

      }
    }

    "return 404" when {
      "RunSummaries for non-existent dataset name and version are queried" in {
        // datasets referenced by runs must exist
        datasetFixture.add(DatasetFactory.getDummyDataset("dataset1", version = 1)) // v1 exists

        val response = sendGet[String](s"$apiUrl/dataset1/2") // but v2 does not
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"POST $apiUrl/{datasetName}/{datasetVersion}" can {
    "return 201" when {
      "new Run is created (basic case)" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset")) // dataset ref'd by the run
        val run = RunFactory.getDummyRun()

        val response = sendPost[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))
        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/runs/dummyDataset/1/1")

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        assertOk(response2)
        response2.getBody shouldBe run
      }
      "created run provides a uniqueId if none is specified" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run = RunFactory.getDummyRun(uniqueId = None)
        val response = sendPost[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))

        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/runs/dummyDataset/1/1")

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        assertOk(response2)
        val body2 = response2.getBody
        body2.uniqueId shouldBe defined
        body2 shouldBe run.copy(uniqueId = body2.uniqueId)
      }
      "created run generates a runId=1 for the first run" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run = RunFactory.getDummyRun(runId = 123) // specified runId is ignored

        val response = sendPost[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))
        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/runs/dummyDataset/1/1")

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        assertOk(response2)
        response2.getBody shouldBe run.copy(runId = 1) // no runs present, so runId = 1
      }
      "created run generates a subsequent runId" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        runFixture.add(
          RunFactory.getDummyRun(runId = 1),
          RunFactory.getDummyRun(runId = 2)
        )
        val run = RunFactory.getDummyRun(runId = 222) // specified runId is ignored, subsequent is used

        val response = sendPost[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run))
        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/runs/dummyDataset/1/3")

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/3")
        assertOk(response2)
        response2.getBody shouldBe run.copy(runId = 3) // runs 1, and 2 were already presents
      }
      "handles two runs being started simultaneously" in {
        datasetFixture.add(DatasetFactory.getDummyDataset("dummyDataset"))
        val run1 = RunFactory.getDummyRun()
        val run2 = RunFactory.getDummyRun()
        run1.uniqueId should not be run2.uniqueId

        // no guarantee which is first
        val response1 = await(sendPostAsync[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run1)))
        val response2 = await(sendPostAsync[Run, String](s"$apiUrl/dummyDataset/1", bodyOpt = Option(run2)))

        Set(response1, response2).foreach(_.getStatusCode shouldBe HttpStatus.CREATED)
        Set(response1, response2).map(resp => stripBaseUrl(resp.getHeaders.getFirst("Location"))) shouldBe
          Set("/runs/dummyDataset/1/1", "/runs/dummyDataset/1/2")

        val retrieved1 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        val retrieved2 = sendGet[Run](s"$apiUrl/dummyDataset/1/2")
        Set(retrieved1, retrieved2).foreach(_.getStatusCode shouldBe HttpStatus.OK)

        retrieved1.getBody shouldBe run1.copy(runId = retrieved1.getBody.runId) // actual runId from the assigned value is used
        retrieved2.getBody shouldBe run2.copy(runId = retrieved2.getBody.runId)
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

        val response = sendPut[RunStatus, MessageWrapper](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(RunStatus(RunState.allSucceeded, None)))
        assertOk(response)
        response.getBody shouldBe MessageWrapper("New runStatus RunStatus(allSucceeded,None) applied.")

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        assertOk(response2)
        response2.getBody shouldBe run.copy(runStatus = RunStatus(RunState.allSucceeded, None))
      }
      "run is updated (running -> failed with error)" in {
        val run = RunFactory.getDummyRun(runStatus = RunStatus(RunState.running, None))
        runFixture.add(run)

        val runError = RunError("job1", "step2", "desc3", "details4")
        val newRunStatus = RunStatus(RunState.failed, Some(runError))
        val response = sendPut[RunStatus, String](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(newRunStatus))
        assertOk(response)
        response.getBody shouldBe
          """{"message":"New runStatus RunStatus(failed,Some(RunError(job1,step2,desc3,details4))) applied."}"""

        val response2 = sendGet[Run](s"$apiUrl/dummyDataset/1/1")
        assertOk(response2)
        response2.getBody shouldBe run.copy(runStatus = newRunStatus)
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
    }
    "return 404" when {
      "a RunState-update references a non-existing run" in {
        // Run ref'd by the run does not exits
        val response = sendPut[RunStatus, Validation](s"$apiUrl/dummyDataset/1/1",
          bodyOpt = Option(RunStatus(RunState.allSucceeded, None)))
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/{runId}/checkpoints" can {
    "return 200" when {
      "return Checkpoints by dataset name, version, and runId (empty)" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        runFixture.add(dataset1ver1run1)

        val response = sendGet[Array[Checkpoint]](s"$apiUrl/dataset1/1/1/checkpoints")
        response.getStatusCode shouldBe HttpStatus.OK

        response.getBody shouldBe Seq.empty[Checkpoint]
      }
      "return Checkpoints by dataset name, version, and runId (non-empty)" in {
        import RunFactory._
        val dataset1ver1run1 = getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
          controlMeasure = RunFactory.getDummyControlMeasure(checkpoints = List(
            RunFactory.getDummyCheckpoint(name = "cp1", order = 0, controls = List(getDummyMeasurement(controlValue = 3))),
            RunFactory.getDummyCheckpoint(name = "cp2", order = 1, controls = List(getDummyMeasurement(controlValue = "asdf")))
          )))
        runFixture.add(dataset1ver1run1)

        val response = sendGet[String](s"$apiUrl/dataset1/1/1/checkpoints")
        response.getStatusCode shouldBe HttpStatus.OK

        response.getBody shouldBe
          """[{
            |"name":"cp1","software":null,"version":null,
            |"processStartTime":"04-12-2017 16:19:17 +0200","processEndTime":"04-12-2017 16:19:17 +0200",
            |"workflowName":"dummyWorkFlowName","order":0,"controls":[
            |{"controlName":"dummyControlName","controlType":"dummyControlType","controlCol":"dummyControlCol","controlValue":3}
            |]
            |},{
            |"name":"cp2","software":null,"version":null,
            |"processStartTime":"04-12-2017 16:19:17 +0200","processEndTime":"04-12-2017 16:19:17 +0200",
            |"workflowName":"dummyWorkFlowName","order":1,"controls":[
            |{"controlName":"dummyControlName","controlType":"dummyControlType","controlCol":"dummyControlCol","controlValue":"asdf"}
            |]
            |}]""".stripMargin.replaceAll("\n", "")
      }
    }
    "return 404" when {
      "run for checkpoint does not exists" in {
        val response = sendGet[String](s"$apiUrl/dataset1/2/3/checkpoints")
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"POST $apiUrl/{datasetName}/{datasetVersion}/{runId}/checkpoints" can {
    "return 201" when {
      "add a new checkpoint" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        runFixture.add(dataset1ver1run1)

        val checkpoint1 = RunFactory.getDummyCheckpoint("cp1")
        val response = sendPost[Checkpoint, String](s"$apiUrl/dataset1/1/1/checkpoints", bodyOpt = Some(checkpoint1))
        response.getStatusCode shouldBe HttpStatus.CREATED
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/runs/dataset1/1/1/checkpoints/cp1")

        val response2 = sendGet[Array[Checkpoint]](s"$apiUrl/dataset1/1/1/checkpoints")
        assertOk(response2)
        response2.getBody shouldBe Seq(checkpoint1)
      }
    }

    "return 400" when {
      "adding a checkpoint duplicate by name" in {
        val checkpoint1 = RunFactory.getDummyCheckpoint("cp1")
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
          controlMeasure = RunFactory.getDummyControlMeasure(checkpoints = List(checkpoint1)))
        runFixture.add(dataset1ver1run1)

        val cp1dupl = checkpoint1.copy(order = 1, workflowName = "wf2") // same name CP

        val response = sendPost[Checkpoint, Validation](s"$apiUrl/dataset1/1/1/checkpoints", bodyOpt = Some(cp1dupl))
        response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
        response.getBody shouldBe Validation.empty.withError("checkpoint.name", "Checkpoint with name cp1 already exists!")
      }
    }

    "return 404" when {
      "run for checkpoint does not exists" in {
        val checkpoint1 = RunFactory.getDummyCheckpoint("cp1")
        val response = sendPost[Checkpoint, String](s"$apiUrl/dataset1/1/1/checkpoints", bodyOpt = Some(checkpoint1))
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/{runId}/checkpoints/{checkpointName}" can {
    "return 200" when {
      "return Checkpoint by dataset name, version, runId, and checkpoint name" in {
        val cp1 = RunFactory.getDummyCheckpoint(name = "cp1", order = 0, controls = List(getDummyMeasurement(controlValue = 3)))
        val cp2 = RunFactory.getDummyCheckpoint(name = "cp2", order = 1, controls = List(getDummyMeasurement(controlValue = "asdf")))
        val dataset1ver1run1 = getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
          controlMeasure = RunFactory.getDummyControlMeasure(checkpoints = List(cp1, cp2))
        )
        runFixture.add(dataset1ver1run1)

        val response = sendGet[Checkpoint](s"$apiUrl/dataset1/1/1/checkpoints/cp1")
        response.getStatusCode shouldBe HttpStatus.OK

        response.getBody shouldBe cp1
      }

    }
    "return 404" when {
      "run does not exists" in {
        val response = sendGet[String](s"$apiUrl/dataset1/2/3/checkpoints/namedCp")
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
      "cp in the run does not exists" in {
        val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
        runFixture.add(dataset1ver1run1)

        val response = sendGet[String](s"$apiUrl/dataset1/1/1/checkpoints/namedCp")
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/{runId}/metadata" can {
    "return 200" when {
      "return Metadata by dataset name, version, and runId" in {
        import RunFactory._
        val testMetadata = getDummyMetadata(additionalInfo = Map("extra_field" -> "extra_value"))
        val dataset1ver1run1 = getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
          controlMeasure = getDummyControlMeasure(metadata = testMetadata)
        )
        runFixture.add(dataset1ver1run1)

        val response = sendGet[ControlMeasureMetadata](s"$apiUrl/dataset1/1/1/metadata")
        response.getStatusCode shouldBe HttpStatus.OK

        response.getBody shouldBe testMetadata
      }
    }
    "return 404" when {
      "run for metadata does not exists" in {
        val response = sendGet[String](s"$apiUrl/dataset1/2/3/metadata")
        response.getStatusCode shouldBe HttpStatus.NOT_FOUND
      }
    }
  }
}
