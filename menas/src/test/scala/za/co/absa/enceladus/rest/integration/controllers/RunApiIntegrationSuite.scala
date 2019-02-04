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

package za.co.absa.enceladus.rest.integration.controllers

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest.Application
import za.co.absa.enceladus.rest.integration.fixtures.RunFixtureService
import za.co.absa.enceladus.rest.models.Validation

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Array(classOf[Application]))
class RunApiIntegrationSuite extends BaseRestApiTest {

  @Autowired
  private val runFixture: RunFixtureService = null

  private val apiUrl = "/runs"

  before {
    runFixture.createCollection()
  }

  after {
    runFixture.dropCollection()
  }

  s"Calls to $apiUrl/list" can {
    "return 200" when {
      "there are Runs" should {
        "return only the latest run of each stored Runs" in {
          val dataset1run1 = runFixture.getDummyRun(dataset = "dataset1", runId = 1)
          val dataset1run2 = runFixture.getDummyRun(dataset = "dataset1", runId = 2)
          runFixture.add(dataset1run1, dataset1run2)
          val dataset2run1 = runFixture.getDummyRun(dataset = "dataset2", runId = 1)
          runFixture.add(dataset2run1)

          val response = sendGet[Array[Run]](s"$apiUrl/list")

          assertOk(response)

          val body = response.getBody
          assert(body.length == 2)
          assert(body.sameElements(Array(dataset1run2, dataset2run1)))
        }
      }

      "there are no Run entities stored in the database" should {
        "return an empty collection" in {
          val response = sendGet[Array[Run]](s"$apiUrl/list")

          assertOk(response)

          val body = response.getBody
          assert(body.isEmpty)
        }
      }
    }
  }

  s"Calls to $apiUrl/startDate/{statDate}" can {
    val startDate = "28-01-2019"

    "return 200" when {
      "there are Runs on the specified startDate" should {
        "return only the latest run for each dataset on that startDate" in {
          val dataset1run1 = runFixture.getDummyRun(dataset = "dataset1", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          val dataset1run2 = runFixture.getDummyRun(dataset = "dataset1", runId = 2, startDateTime = s"$startDate 14:01:12 +0200")
          runFixture.add(dataset1run1, dataset1run2)
          val dataset2run1 = runFixture.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
          runFixture.add(dataset2run1)

          val response = sendGet[Array[Run]](s"$apiUrl/startDate/$startDate")

          assertOk(response)

          val body = response.getBody
          assert(body.length == 2)
          assert(body.sameElements(Array(dataset1run2, dataset2run1)))
        }
      }

      "there are no Runs for the specified startDate" should {
        "return an empty collection" in {
          val run = runFixture.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
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
          val run = runFixture.getDummyRun(startDateTime = s"$startDate 13:01:12 +0200")
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

  s"Calls to $apiUrl/{datasetName}/{datasetVersion}/{runId}" can {
    "return 200" when {
      "there is a Run of the specified Dataset with the specified runId" should {
        "return the Run" in {
          val dataset1run1 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
          val dataset2run2 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[Run](s"$apiUrl/dataset/1/2")

          assertOk(response)

          val body = response.getBody
          assert(body == dataset1run2)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified datasetName" in {
        val run = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        runFixture.add(run)

        val response = sendGet[Run](s"$apiUrl/DATASET/1/1")

        assertNotFound(response)
      }
      "there is no Run with the specified datasetVersion" in {
        val run = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        runFixture.add(run)

        val response = sendGet[Run](s"$apiUrl/dataset/2/1")

        assertNotFound(response)
      }
      "there is no Run with the specified runId" in {
        val run = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        runFixture.add(run)

        val response = sendGet[Run](s"$apiUrl/dataset/1/2")

        assertNotFound(response)
      }
    }
  }

  s"Calls to $apiUrl/{datasetName}/{datasetVersion}/latest" can {
    "return 200" when {
      "there are Runs with the specified datasetName and datasetVersion" should {
        "return the Run with the latest RunId" in {
          val dataset1run1 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
          val dataset1run2 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
          val dataset2run2 = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
          runFixture.add(dataset1run1, dataset1run2, dataset2run2)

          val response = sendGet[Run](s"$apiUrl/dataset/1/latest")

          assertOk(response)

          val body = response.getBody
          assert(body == dataset1run2)
        }
      }
    }

    "return 404" when {
      "there is no Run with the specified datasetName" in {
        val run = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        runFixture.add(run)

        val response = sendGet[Run](s"$apiUrl/DATASET/1/latest")

        assertNotFound(response)
      }
      "there is no Run with the specified datasetVersion" in {
        val run = runFixture.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        runFixture.add(run)

        val response = sendGet[Run](s"$apiUrl/dataset/2/latest")

        assertNotFound(response)
      }
    }
  }

}
