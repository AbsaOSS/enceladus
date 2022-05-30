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
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunState, RunStatus}
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.enceladus.model.test.factories.RunFactory
import za.co.absa.enceladus.model.{Run, SplineReference, Validation}
import za.co.absa.enceladus.rest_api.integration.controllers.BaseRestApiTestV3
import za.co.absa.enceladus.rest_api.integration.fixtures.{FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary}

import java.util.UUID

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class RunControllerV3IntegrationSuite extends BaseRestApiTestV3 with Matchers {

  import za.co.absa.enceladus.model.Validation._
  import za.co.absa.enceladus.rest_api.integration.RunImplicits.RunExtensions

  @Autowired
  private val runFixture: RunFixtureService = null

  override def fixtures: List[FixtureService[_]] = List(runFixture)

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
        val expected = Array(run1).map(_.toSummary)
        response.getBody shouldBe expected
      }

      "latest RunSummaries are queried, but nothing is found" in {
        val run1 = RunFactory.getDummyRun(dataset = "dataset1", startDateTime = "22-05-2022 14:01:12 +0200")
        val run2 = RunFactory.getDummyRun(dataset = "dataset3", uniqueId = None) // unrelated to dataset1
        runFixture.add(run1, run2)

        val response = sendGet[String](s"$apiUrl/dataset1?startDate=24-05-2022")
        response.getBody shouldBe "[]" // empty array
      }
    }

  }

   // todo cover @GetMapping(Array("/{datasetName}/{datasetVersion}"))

  // todo add other endpoints test cases

}
