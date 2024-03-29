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

package za.co.absa.enceladus.rest_api.integration.controllers

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunState, RunStatus}
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.enceladus.rest_api.integration.fixtures.{FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary}
import za.co.absa.enceladus.model.test.factories.RunFactory
import za.co.absa.enceladus.model.{Run, SplineReference, Validation}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class RunApiIntegrationSuite extends BaseRestApiTestV2 {

  import za.co.absa.enceladus.rest_api.integration.RunImplicits.RunExtensions
  import za.co.absa.enceladus.model.Validation._

  @Autowired
  private val runFixture: RunFixtureService = null

  override def fixtures: List[FixtureService[_]] = List(runFixture)

  private val apiUrl = "/runs"

  s"GET $apiUrl" can {
    "return 200" when {
      "there are Runs" should {
        "return only the latest Run of each Dataset" in {
          val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
          val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
          runFixture.add(dataset1ver1run1, dataset1ver1run2)
          val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
          runFixture.add(dataset1ver2run1)
          val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
          runFixture.add(dataset2ver1run1)

          val response = sendGet[String](s"$apiUrl")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(Array(dataset1ver2run1, dataset2ver1run1)))
        }
        "order the results by dataset name (ASC)" in {
          val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
          runFixture.add(dataset2ver1run1)
          val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
          runFixture.add(dataset1ver2run1)
          val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
          val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
          runFixture.add(dataset1ver1run1, dataset1ver1run2)

          val response = sendGet[String](s"$apiUrl")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(Array(dataset1ver2run1, dataset2ver1run1)))
        }
        "serialize the Runs correctly" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1)
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2,
            uniqueId = Some("14ff63a4-c836-4260-871e-1edc8c8e205e"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("e736e0ce-64b3-4b4a-8ddc-1176ba645519")))
          runFixture.add(dataset1run1, dataset1run2)
          val dataset2run1 = RunFactory.getDummyRun(dataset = "dataset2", runId = 1,
            uniqueId = Some("6e4a3573-1ee3-42bc-8fe1-391d9b61bf57"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("534241a6-2bea-468d-8c9c-8d2601055f28")))
          runFixture.add(dataset2run1)

          val response = sendGet[String](s"$apiUrl")

          assertOk(response)

          val body = response.getBody
          val dataset1run2json = getDummyRunJson(
            dataset = "dataset1",
            datasetVersion = 1,
            runId = 2,
            uniqueId = "14ff63a4-c836-4260-871e-1edc8c8e205e",
            runUniqueId = "e736e0ce-64b3-4b4a-8ddc-1176ba645519")
          val dataset2run1json = getDummyRunJson(
            dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            uniqueId = "6e4a3573-1ee3-42bc-8fe1-391d9b61bf57",
            runUniqueId = "534241a6-2bea-468d-8c9c-8d2601055f28")
          val expected = s"""[$dataset1run2json,$dataset2run1json]"""

          assert(body == expected)
        }
      }

      "there are no Run entities stored in the database" should {
        "return an empty collection" in {
          val response = sendGet[Array[Run]](s"$apiUrl")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/startDate/{startDate}" can {
    val startDate = "28-01-2019"

    "return 200" when {
      "there are Runs on the specified startDate" should {
        "return only the latest run for each dataset on that startDate" in {
          val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = s"$startDate 14:01:12 +0200")
          runFixture.add(dataset1ver1run1, dataset1ver1run2)
          val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = s"$startDate 15:01:12 +0200")
          runFixture.add(dataset1ver2run1)
          val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          runFixture.add(dataset2ver1run1)
          val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1,startDateTime = "29-01-2019 13:01:12 +0200")
          runFixture.add(dataset3ver1run1)

          val response = sendGet[String](s"$apiUrl/startDate/$startDate")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(Array(dataset1ver2run1, dataset2ver1run1)))
        }
        "order the results by dataset name (ASC)" in {
          val dataset3ver1run1 = RunFactory.getDummyRun(dataset = "dataset3", datasetVersion = 1, runId = 1, startDateTime = "29-01-2019 13:01:12 +0200")
          runFixture.add(dataset3ver1run1)
          val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          runFixture.add(dataset2ver1run1)
          val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = s"$startDate 15:01:12 +0200")
          runFixture.add(dataset1ver2run1)
          val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = s"$startDate 14:01:12 +0200")
          runFixture.add(dataset1ver1run1, dataset1ver1run2)

          val response = sendGet[String](s"$apiUrl/startDate/$startDate")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(Array(dataset1ver2run1, dataset2ver1run1)))
        }
        "serialize the Runs correctly" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, startDateTime = s"$startDate 14:01:12 +0200",
            uniqueId = Some("14ff63a4-c836-4260-871e-1edc8c8e205e"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("e736e0ce-64b3-4b4a-8ddc-1176ba645519")))
          runFixture.add(dataset1run1, dataset1run2)
          val dataset2run1 = RunFactory.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = s"$startDate 13:01:12 +0200",
            uniqueId = Some("6e4a3573-1ee3-42bc-8fe1-391d9b61bf57"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("534241a6-2bea-468d-8c9c-8d2601055f28")))
          runFixture.add(dataset2run1)
          val dataset3run1 = RunFactory.getDummyRun(dataset = "dataset3", runId = 1, startDateTime = "29-01-2019 13:01:12 +0200")
          runFixture.add(dataset3run1)


          val response = sendGet[String](s"$apiUrl/startDate/$startDate")

          assertOk(response)

          val body = response.getBody
          val dataset1run2json = getDummyRunJson(
            dataset = "dataset1",
            datasetVersion = 1,
            runId = 2,
            uniqueId = "14ff63a4-c836-4260-871e-1edc8c8e205e",
            runUniqueId = "e736e0ce-64b3-4b4a-8ddc-1176ba645519",
            startDateTime = s"$startDate 14:01:12 +0200")
          val dataset2run1json = getDummyRunJson(
            dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            uniqueId = "6e4a3573-1ee3-42bc-8fe1-391d9b61bf57",
            runUniqueId = "534241a6-2bea-468d-8c9c-8d2601055f28",
            startDateTime = s"$startDate 13:01:12 +0200")
          val expected = s"""[$dataset1run2json,$dataset2run1json]"""

          assert(body == expected)

        }
      }

      "there are no Runs for the specified startDate" should {
        "return an empty collection" in {
          val run = RunFactory.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
          runFixture.add(run)

          val response = sendGet[Array[Run]](s"$apiUrl/startDate/$startDate")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }

    "return 400" when {
      "the startDate does not have the format dd-MM-yyyy" should {
        "return a Validation error" in {
          val run = RunFactory.getDummyRun(startDateTime = s"$startDate 13:01:12 +0200")
          runFixture.add(run)

          val response = sendGet[Validation](s"$apiUrl/startDate/01-29-2019")

          assertBadRequest(response)

          val body = response.getBody
          assert(!body.isValid)
          assert(body == Validation().withError("startDate", "must have format dd-MM-yyyy: 01-29-2019"))
        }
      }
    }
  }

  s"GET $apiUrl/summaries" can {
    "return 200" when {
      "there are Run entities in the database" should {
        "return a Summary of each Run" in {
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2,
            runStatus = RunStatus(RunState.running, None))
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/summaries")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1).map(_.toSummary)
          assert(body.sameElements(expected))
        }

        "order RunSummaries by Dataset Name (ASC), Dataset Version (ASC), Run ID (ASC)" in {
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(dataset2v1run1)
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))
          runFixture.add(dataset1v2run1)
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2,
            runStatus = RunStatus(RunState.running, None))
          runFixture.add(dataset1v1run2)
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          runFixture.add(dataset1v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/summaries")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1).map(_.toSummary)
          assert(body.sameElements(expected))
        }
      }

      "there are no Run entities stored in the database" should {
        "return an empty collection" in {
          val response = sendGet[Array[RunSummary]](s"$apiUrl/summaries")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/grouped" can {
    "return 200" when {
      "there are Run entities in the database" should {
        "return a Summary of each Run" in {
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 12:00:00 +0200")
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "04-12-2018 13:00:00 +0200")
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 16:19:17 +0200")
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 06:00:00 +0200")
          val dataset2v2run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 06:00:00 +0200")
          val dataset2v2run2 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 2, runId = 2, startDateTime = "05-12-2018 06:00:00 +0200")
          runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1, dataset2v2run1, dataset2v2run2)

          val response = sendGet[Array[RunDatasetNameGroupedSummary]](s"$apiUrl/grouped")

          assertOk(response)

          val body = response.getBody
          val dataset1Summary = RunDatasetNameGroupedSummary("dataset1", 3, "04-12-2018 16:19:17 +0200")
          val dataset2Summary = RunDatasetNameGroupedSummary("dataset2", 3, "05-12-2018 06:00:00 +0200")
          val expected = List(dataset1Summary, dataset2Summary)
          assert(body.sameElements(expected))
        }

        "order RunSummaries by Dataset Name (ASC)" in {
          val dataset2v2run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 06:00:00 +0200")
          runFixture.add(dataset2v2run1)
          val dataset2v2run2 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 2, runId = 2, startDateTime = "05-12-2018 06:00:00 +0200")
          runFixture.add(dataset2v2run2)
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 06:00:00 +0200")
          runFixture.add(dataset2v1run1)
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 16:19:17 +0200")
          runFixture.add(dataset1v2run1)
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 12:00:00 +0200")
          runFixture.add(dataset1v1run1)
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2, startDateTime = "04-12-2018 13:00:00 +0200")
          runFixture.add(dataset1v1run2)

          val response = sendGet[Array[RunDatasetNameGroupedSummary]](s"$apiUrl/grouped")

          assertOk(response)

          val body = response.getBody
          val dataset1Summary = RunDatasetNameGroupedSummary("dataset1", 3, "04-12-2018 16:19:17 +0200")
          val dataset2Summary = RunDatasetNameGroupedSummary("dataset2", 3, "05-12-2018 06:00:00 +0200")
          val expected = List(dataset1Summary, dataset2Summary)
          assert(body.sameElements(expected))
        }
      }

      "there are no Run entities stored in the database" should {
        "return an empty collection" in {
          val response = sendGet[Array[RunDatasetNameGroupedSummary]](s"$apiUrl/grouped")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/grouped/{datasetName}" can {
    val queriedDatasetName = "dataset1"
    val wrongDatasetName = "dataset2"

    "return 200" when {
      "there are Run entities in the database" should {
        "return a Summary of each Run" in {
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 12:00:00 +0200")
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 2, startDateTime = "04-12-2018 13:00:00 +0200")
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 16:19:17 +0200")
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 06:00:00 +0200")
          val dataset2v2run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 06:00:00 +0200")
          val dataset2v2run2 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 2, runId = 2, startDateTime = "05-12-2018 06:00:00 +0200")
          runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1, dataset2v2run1, dataset2v2run2)

          val response = sendGet[Array[RunDatasetVersionGroupedSummary]](s"$apiUrl/grouped/$queriedDatasetName")

          assertOk(response)

          val body = response.getBody
          val dataset1v1Summary = RunDatasetVersionGroupedSummary(queriedDatasetName, 1, 2, "04-12-2018 13:00:00 +0200")
          val dataset1v2Summary = RunDatasetVersionGroupedSummary(queriedDatasetName, 2, 1, "04-12-2018 16:19:17 +0200")
          val expected = List(dataset1v2Summary, dataset1v1Summary)
          assert(body.sameElements(expected))
        }

        "order RunSummaries by Dataset Version (ASC)" in {
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 2, runId = 1, startDateTime = "04-12-2018 16:19:17 +0200")
          runFixture.add(dataset1v2run1)
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 1, startDateTime = "03-12-2018 12:00:00 +0200")
          runFixture.add(dataset1v1run1)

          val response = sendGet[Array[RunDatasetVersionGroupedSummary]](s"$apiUrl/grouped/$queriedDatasetName")

          assertOk(response)

          val body = response.getBody
          val dataset1v1Summary = RunDatasetVersionGroupedSummary(queriedDatasetName, 1, 1, "03-12-2018 12:00:00 +0200")
          val dataset1v2Summary = RunDatasetVersionGroupedSummary(queriedDatasetName, 2, 1, "04-12-2018 16:19:17 +0200")
          val expected = List(dataset1v2Summary, dataset1v1Summary)
          assert(body.sameElements(expected))
        }
      }

      "there are no Run entities stored in the database" should {
        "return an empty collection" in {
          val response = sendGet[Array[RunDatasetVersionGroupedSummary]](s"$apiUrl/grouped/dataset1")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/{datasetName}" can {
    val queriedDatasetName = "dataset1"
    val wrongDatasetName = "dataset2"

    "return 200" when {
      "there are Runs with the specified Dataset Name" should {
        "return a Summary of each Run" in {
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 2,
            runStatus = RunStatus(RunState.running, None))
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 2, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))

          val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2, dataset1v2run1).map(_.toSummary)
          assert(body.sameElements(expected))
        }

        "order RunSummaries by Dataset Version (ASC), Run ID (ASC)" in {
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))
          runFixture.add(dataset1v2run1)
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2,
            runStatus = RunStatus(RunState.running, None))
          runFixture.add(dataset1v1run2)
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          runFixture.add(dataset1v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2, dataset1v2run1).map(_.toSummary)
          assert(body.sameElements(expected))

        }
      }

      "there are no Runs with the specified Dataset Name" should {
        "return an empty collection" in {
          val run = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(run)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}" can {
    val queriedDatasetName = "dataset1"
    val wrongDatasetName = "dataset2"

    val queriedDatasetVersion = 1
    val wrongDatasetVersion = 2

    "return 200" when {
      "there are Runs with the specified Dataset Name and Version" should {
        "return a Summary of each Run" in {
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = queriedDatasetVersion, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = queriedDatasetVersion, runId = 2,
            runStatus = RunStatus(RunState.running, None))

          val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = wrongDatasetVersion, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = queriedDatasetVersion, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName/$queriedDatasetVersion")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2).map(_.toSummary)
          assert(body.sameElements(expected))
        }

        "order RunSummaries by Run ID (ASC)" in {
          val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2,
            runStatus = RunStatus(RunState.running, None))
          runFixture.add(dataset1v1run2)
          val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
            runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())))
          runFixture.add(dataset1v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName/$queriedDatasetVersion")

          assertOk(response)

          val body = response.getBody
          val expected = List(dataset1v1run1, dataset1v1run2).map(_.toSummary)
          assert(body.sameElements(expected))
        }
      }

      "there are no Runs with the specified Dataset Name and Version" should {
        "return an empty collection" in {
          val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = wrongDatasetVersion, runId = 1,
            runStatus = RunStatus(RunState.stageSucceeded, None))
          val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = queriedDatasetVersion, runId = 1,
            runStatus = RunStatus(RunState.allSucceeded, None))
          runFixture.add(dataset1v2run1, dataset2v1run1)

          val response = sendGet[Array[RunSummary]](s"$apiUrl/$queriedDatasetName/$queriedDatasetVersion")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/{runId}" can {
    "return 200" when {
      "there is a Run of the specified Dataset with the specified runId" should {
        "return the Run" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
          val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[String](s"$apiUrl/dataset/1/2")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(dataset1run2))
        }
        "serialize the Run correctly" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2,
            uniqueId = Some("14ff63a4-c836-4260-871e-1edc8c8e205e"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("e736e0ce-64b3-4b4a-8ddc-1176ba645519")))
          val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[String](s"$apiUrl/dataset/1/2")

          assertOk(response)

          val body = response.getBody
          val expected = getDummyRunJson(
            dataset = "dataset",
            datasetVersion = 1,
            runId = 2,
            uniqueId = "14ff63a4-c836-4260-871e-1edc8c8e205e",
            runUniqueId = "e736e0ce-64b3-4b4a-8ddc-1176ba645519")
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified datasetName" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/DATASET/1/1")

        assertNotFound(response)
      }
      "there is no Run with the specified datasetVersion" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/dataset/2/1")

        assertNotFound(response)
      }
      "there is no Run with the specified runId" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/dataset/1/2")

        assertNotFound(response)
      }
      "the datasetVersion is not a valid numeric type" in {
        setUpSimpleRun()

        val response = sendGet[String](s"$apiUrl/dataset/datasetVersion/1")

        assertNotFound(response)
      }
      "the runId is not a valid numeric type" in {
        setUpSimpleRun()

        val response = sendGet[String](s"$apiUrl/dataset/1/runId")

        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/{datasetName}/{datasetVersion}/latestrun" can {
    "return 200" when {
      "there are Runs with the specified datasetName and datasetVersion" should {
        "return the Run with the latest RunId" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
          val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[String](s"$apiUrl/dataset/1/latestrun")

          assertOk(response)

          val body = response.getBody
          assert(body == SerializationUtils.asJson(dataset1run2))
        }
        "serialize the Run correctly" in {
          val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2,
            uniqueId = Some("14ff63a4-c836-4260-871e-1edc8c8e205e"),
            controlMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Some("e736e0ce-64b3-4b4a-8ddc-1176ba645519")))
          val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[String](s"$apiUrl/dataset/1/latestrun")

          assertOk(response)

          val body = response.getBody
          val expected = getDummyRunJson(
            dataset = "dataset",
            datasetVersion = 1,
            runId = 2,
            uniqueId = "14ff63a4-c836-4260-871e-1edc8c8e205e",
            runUniqueId = "e736e0ce-64b3-4b4a-8ddc-1176ba645519")
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified datasetName" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/DATASET/1/latestrun")

        assertNotFound(response)
      }
      "there is no Run with the specified datasetVersion" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/dataset/2/latestrun")

        assertNotFound(response)
      }
      "the datasetVersion is not a valid numeric type" in {
        setUpSimpleRun()

        val response = sendGet[Run](s"$apiUrl/dataset/datasetVersion/latestrun")

        assertNotFound(response)
      }
    }
  }

  s"POST $apiUrl" can {
    val endpointBase = s"$apiUrl"

    "return 201" when {
      "a new Run is created" should {
        "return the created Run" in {
          val run = RunFactory.getDummyRun()

          val response = sendPost[Run, Run](endpointBase, bodyOpt = Option(run))

          assertCreated(response)

          val body = response.getBody
          assert(body == run)
        }
        "provide a uniqueId if none is specified" in {
          val run = RunFactory.getDummyRun(uniqueId = None)

          val response = sendPost[Run, Run](endpointBase, bodyOpt = Option(run))

          assertCreated(response)

          val body = response.getBody
          assert(body.uniqueId.isDefined)
          val expected = run.copy(uniqueId = body.uniqueId)
          assert(body == expected)
        }
        "override any specified runId in favor of a generated one" which {
          "is 1 on first run for the given Dataset Name and Version" in {
            val run = RunFactory.getDummyRun()

            val response = sendPost[Run, Run](endpointBase, bodyOpt = Option(run))

            assertCreated(response)

            val body = response.getBody
            assert(body == run)
          }
          "is the latest previous run + 1 on subsequent runs for the given Dataset Name and Version" in {
            val run = RunFactory.getDummyRun()

            val response = sendPost[Run, Run](endpointBase, bodyOpt = Option(run))

            assertCreated(response)

            val body = response.getBody
            assert(body == run)
          }
          "handles two runs being started simultaneously" in {
            val run1 = RunFactory.getDummyRun()
            val run2 = RunFactory.getDummyRun()

            val eventualResponse1 = sendPostAsync[Run, Run](endpointBase, bodyOpt = Option(run1))
            val eventualResponse2 = sendPostAsync[Run, Run](endpointBase, bodyOpt = Option(run2))

            val response1 = await(eventualResponse1)
            val response2 = await(eventualResponse2)

            assertCreated(response1)
            assertCreated(response2)

            val body1 = response1.getBody
            val body2 = response2.getBody
            assert(body1 == run1.copy(runId = body1.runId))
            assert(body2 == run2.copy(runId = body2.runId))
            assert(Array(body1.runId, body2.runId).sorted.sameElements(Array(1, 2)))
          }
        }
      }
    }

    "return 400" when {
      "receiving an empty JSON" in {
        val response = sendPost[String, Validation](endpointBase, bodyOpt = Option("{}"))

        assertBadRequest(response)

        val body = response.getBody
        assert(!body.isValid)
        assert(body == Validation()
          .withError("dataset", NotSpecified)
          .withError("datasetVersion", NotSpecified))
      }
      "a Run with the given uniqueId already exists" in {
        val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"
        val presentRun = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
        runFixture.add(presentRun)
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))

        val response = sendPost[Run, Validation](endpointBase, bodyOpt = Option(run))

        assertBadRequest(response)

        val body = response.getBody
        assert(!body.isValid)
        assert(body == Validation().withError("uniqueId", s"run with this uniqueId already exists: $uniqueId"))
      }
    }
  }

  s"POST $apiUrl/addCheckpoint/{uniqueId}" can {
    val endpointBase = s"$apiUrl/addCheckpoint"
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "return 200" when {
      "there is a Run with the specified uniqueId" should {
        "add the supplied checkpoint to the end of the present checkpoints and return the updated Run" in {
          val checkpoint0 = RunFactory.getDummyCheckpoint(name = "checkpoint0")
          val measure = RunFactory.getDummyControlMeasure(checkpoints = List(checkpoint0))
          val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), controlMeasure = measure)
          runFixture.add(run)

          val checkpoint1 = RunFactory.getDummyCheckpoint(name = "checkpoint1")

          val response = sendPost[Checkpoint, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(checkpoint1))

          assertOk(response)

          val expectedControlMeasure = run.controlMeasure.copy(checkpoints = List(checkpoint0, checkpoint1))
          val expected = run.copy(controlMeasure = expectedControlMeasure)
          val body = response.getBody
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified uniqueId" in {
        val checkpoint = RunFactory.getDummyCheckpoint()

        val response = sendPost[Checkpoint, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(checkpoint))

        assertNotFound(response)
      }
    }
  }

  s"POST $apiUrl/updateControlMeasure/{uniqueId}" can {
    val endpointBase = s"$apiUrl/updateControlMeasure"
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "return 200" when {
      "there is a Run with the specified uniqueId" should {
        "update the Run's ControlMeasure and return the updated Run" in {
          val originalMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Option("eeeeeeee-f9ac-46f8-9657-a09a4e3fb6e9"))
          val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), controlMeasure = originalMeasure)
          runFixture.add(run)

          val expectedMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Option(uniqueId))

          val response = sendPost[ControlMeasure, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(expectedMeasure))

          assertOk(response)

          val expected = run.copy(controlMeasure = expectedMeasure)
          val body = response.getBody
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified uniqueId" in {
        val controlMeasure = RunFactory.getDummyControlMeasure()

        val response = sendPost[ControlMeasure, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(controlMeasure))

        assertNotFound(response)
      }
    }
  }

  s"POST $apiUrl/updateSplineReference/{uniqueId}" can {
    val endpointBase = s"$apiUrl/updateSplineReference"
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "return 200" when {
      "there is a Run with the specified uniqueId" should {
        "update the Run's SplineReference and return the updated Run" in {
          val originalSplineRef = RunFactory.getDummySplineReference(sparkApplicationId = null)
          val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), splineRef = originalSplineRef)
          runFixture.add(run)

          val expectedSplineRef = RunFactory.getDummySplineReference(sparkApplicationId = "application_1512977199009_0007")

          val response = sendPost[SplineReference, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(expectedSplineRef))

          assertOk(response)

          val expected = run.copy(splineRef = expectedSplineRef)
          val body = response.getBody
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified uniqueId" in {
        val splineReference = RunFactory.getDummySplineReference()

        val response = sendPost[SplineReference, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(splineReference))

        assertNotFound(response)
      }
    }
  }

  s"POST $apiUrl/updateRunStatus/{uniqueId}" can {
    val endpointBase = s"$apiUrl/updateRunStatus"
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "return 200" when {
      "there is a Run with the specified uniqueId" should {
        "update the Run's RunStatus and return the updated Run" in {
          val originalStatus = RunFactory.getDummyRunStatus(runState = RunState.running)
          val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), runStatus = originalStatus)
          runFixture.add(run)

          val expectedStatus = RunFactory.getDummyRunStatus(runState = RunState.allSucceeded)

          val response = sendPost[RunStatus, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(expectedStatus))

          assertOk(response)

          val expected = run.copy(runStatus = expectedStatus)
          val body = response.getBody
          assert(body == expected)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified uniqueId" in {
        val runStatus = RunFactory.getDummyRunStatus()

        val response = sendPost[RunStatus, Run](s"$endpointBase/$uniqueId", bodyOpt = Option(runStatus))

        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/bySparkAppId/{appId}" can {
    val sampleAppId1 = "application_1578585424019_0008" // YARN
    val sampleAppId2 = "local-1433865536131"            // local
    val sampleAppId3 = "driver-20170926223339-0001"     // MESOS

    "return 200" when {
      "there are no Run entities stored in the database" should {
        "return an empty collection" in{
          val response = sendGet[Array[Run]](s"$apiUrl/bySparkAppId/$sampleAppId1")
          assertOk(response)
          val body = response.getBody
          assert(body.isEmpty)
        }
      }

      "there are Run entities stored in the database" should{
        "return empty collection when searching for missing app_id" in {
          val run1 = setUpRunWithAppIds(sampleAppId1, runId = 1)
          val wrongAppId = "missing-100500777-0042"     // MESOS
          val response = sendGet[Array[Run]](s"$apiUrl/bySparkAppId/$wrongAppId")
          assertOk(response)
          val body = response.getBody
          assert(body.isEmpty)
        }

        "return [correctRun] when searching for unique app_id" in {
          // std app_id only
          val run1 = setUpRunWithAppIds(sampleAppId1, runId = 1)
          // both std and cnfrm app_ids
          val run2 = setUpRunWithAppIds(sampleAppId2, sampleAppId3, runId = 2)
          // get run1 by std app_id
          val response1 = sendGet[String](s"$apiUrl/bySparkAppId/$sampleAppId1")
          val body1 = response1.getBody
          assert(body1 == SerializationUtils.asJson(Seq(run1)))
          // get run2 by std app_id
          val response2 = sendGet[String](s"$apiUrl/bySparkAppId/$sampleAppId2")
          val body2 = response2.getBody
          assert(body2 == SerializationUtils.asJson(Seq(run2)))
          // get run2 by conform app_id
          val response3 = sendGet[String](s"$apiUrl/bySparkAppId/$sampleAppId3")
          val body3 = response3.getBody
          assert(body3 == SerializationUtils.asJson(Seq(run2)))
        }

        "return [run1, run2] when there are 2 runs with the same app_ids" in {
          val run1 = setUpRunWithAppIds(sampleAppId1, runId = 1)
          val run2 = setUpRunWithAppIds(sampleAppId1, runId = 2)

          // get run1 by std app_id
          val response = sendGet[String](s"$apiUrl/bySparkAppId/$sampleAppId1")
          val actual = response.getBody
          val expected = SerializationUtils.asJson(Seq(run1, run2))
          assert(actual == expected)
        }
      }
    }
  }

  private def setUpSimpleRun(): Run = {
    val run = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
    runFixture.add(run)
    run
  }

  private def getDummyRunJson(dataset: String,
                              datasetVersion: Int,
                              runId: Int,
                              uniqueId: String,
                              runUniqueId: String,
                              startDateTime: String = RunFactory.dummyDateString): String = {
    s"""{"uniqueId":"$uniqueId","runId":$runId,"dataset":"$dataset","datasetVersion":$datasetVersion,""" +
      s""""splineRef":{"sparkApplicationId":"dummySparkApplicationId","outputPath":"dummyOutputPath"},"startDateTime":"$startDateTime",""" +
      """"runStatus":{"status":"allSucceeded"},""" +
      """"controlMeasure":{"metadata":{"sourceApplication":"dummySourceApplication","country":"dummyCountry","historyType":"dummyHistoryType",""" +
      """"dataFilename":"dummyDataFilename","sourceType":"dummySourceType","version":1,"informationDate":"04-12-2017 16:19:17 +0200","additionalInfo":{}},""" +
      s""""runUniqueId":"$runUniqueId","checkpoints":[]}}"""
  }

  private def setUpRunWithAppIds(stdAppId: String, cnfrmAppId: String = "", runId: Int = 1): Run = {
    val additionalInfo: Map[String, String] = if (cnfrmAppId == "") {
      Map("std_application_id" -> stdAppId)
    } else{
      Map("std_application_id" -> stdAppId, "conform_application_id" -> cnfrmAppId)
    }
    val metadata = RunFactory.getDummyMetadata(additionalInfo = additionalInfo)
    val controlMeasure = RunFactory.getDummyControlMeasure(metadata=metadata)
    val run = RunFactory.getDummyRun(runId = runId, controlMeasure = controlMeasure)
    runFixture.add(run)
    run
  }

}
