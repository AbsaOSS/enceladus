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

package za.co.absa.enceladus.rest.integration.repositories

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.rest.Application
import za.co.absa.enceladus.rest.integration.fixtures.RunFixtureService
import za.co.absa.enceladus.rest.repositories.RunMongoRepository

import scala.concurrent.Await

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Array(classOf[Application]))
class RunRepositoryIntegrationSuite extends BaseRepositoryTest {

  @Autowired
  private val runFixture: RunFixtureService = null

  @Autowired
  private val runMongoRepository: RunMongoRepository = null

  before {
    runFixture.createCollection()
  }

  after {
    runFixture.dropCollection()
  }

  "RunMongoRepository::getAllLatest" when {
    "there are Runs" should {
      "asynchronously return only the latest Run for each Dataset" in {
        val dataset1run1 = runFixture.getDummyRun(dataset = "dataset1", runId = 1)
        val dataset1run2 = runFixture.getDummyRun(dataset = "dataset1", runId = 2)
        runFixture.add(dataset1run1, dataset1run2)
        val dataset2run1 = runFixture.getDummyRun(dataset = "dataset2", runId = 1)
        runFixture.add(dataset2run1)

        val actual = Await.result(runMongoRepository.getAllLatest(), awaitDuration)

        val expected = List(dataset1run2, dataset2run1)
        assert(actual == expected)
      }
    }

    "there are no Runs" should {
      "asynchronously return and empty List" in {
        val actual = Await.result(runMongoRepository.getAllLatest(), awaitDuration)

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getByStartDate" when {
    val startDate = "28-01-2019"

    "there are Runs on the specified startDate" should {
      "return only the latest run for each dataset on that startDate" in {
        val dataset1run1 = runFixture.getDummyRun(dataset = "dataset1", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
        val dataset1run2 = runFixture.getDummyRun(dataset = "dataset1", runId = 2, startDateTime = s"$startDate 14:01:12 +0200")
        runFixture.add(dataset1run1, dataset1run2)
        val dataset2run1 = runFixture.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
        runFixture.add(dataset2run1)
        val dataset3run1 = runFixture.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(dataset3run1)

        val actual = Await.result(runMongoRepository.getByStartDate(startDate), awaitDuration)

        val expected = List(dataset1run2, dataset2run1)
        assert(actual == expected)
      }
    }

    "there are no Runs for the specified startDate" should {
      "return an empty collection" in {
        val run = runFixture.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(run)

        val actual = Await.result(runMongoRepository.getByStartDate(startDate), awaitDuration)

        assert(actual.isEmpty)
      }
    }

    "the specified startDate is not a valid date" should {
      "return an empty collection" in {
        val run = runFixture.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(run)

        val actual = Await.result(runMongoRepository.getByStartDate("startDate"), awaitDuration)

        assert(actual.isEmpty)
      }
    }
  }

}
