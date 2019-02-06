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
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest.Application
import za.co.absa.enceladus.rest.factories.RunFactory
import za.co.absa.enceladus.rest.integration.fixtures.RunFixtureService
import za.co.absa.enceladus.rest.repositories.RunMongoRepository

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

  "RunMongoRepository::getAllLatest" should {
    "return only the latest Run for each Dataset asynchronously" when {
      "there are Runs" in {
        val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1)
        val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2)
        runFixture.add(dataset1run1, dataset1run2)
        val dataset2run1 = RunFactory.getDummyRun(dataset = "dataset2", runId = 1)
        runFixture.add(dataset2run1)

        val actual = await(runMongoRepository.getAllLatest())

        val expected = List(dataset1run2, dataset2run1)
        assert(actual == expected)
      }
    }

    "return and empty List asynchronously" when {
      "there are no Runs" in {
        val actual = await(runMongoRepository.getAllLatest())

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getByStartDate" should {
    val startDate = "28-01-2019"

    "return only the latest run for each dataset on that startDate asynchronously" when {
      "there are Runs on the specified startDate" in {
        val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
        val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, startDateTime = s"$startDate 14:01:12 +0200")
        runFixture.add(dataset1run1, dataset1run2)
        val dataset2run1 = RunFactory.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = s"$startDate 13:01:12 +0200")
        runFixture.add(dataset2run1)
        val dataset3run1 = RunFactory.getDummyRun(dataset = "dataset2", runId = 1, startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(dataset3run1)

        val actual = await(runMongoRepository.getByStartDate(startDate))

        val expected = List(dataset1run2, dataset2run1)
        assert(actual == expected)
      }
    }

    "return an empty collection asynchronously" when {
      "there are no Runs for the specified startDate" in {
        val run = RunFactory.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(run)

        val actual = await(runMongoRepository.getByStartDate(startDate))

        assert(actual.isEmpty)
      }

      "the specified startDate is not a valid date" in {
        val run = RunFactory.getDummyRun(startDateTime = "29-01-2019 13:01:12 +0200")
        runFixture.add(run)

        val actual = await(runMongoRepository.getByStartDate("startDate"))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getRun" should {
    "return an Option of the Run asynchronously" when {
      "there is a Run of the specified Dataset with the specified runId" in {
        val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
        val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
        runFixture.add(dataset1run1, dataset1run2, dataset2run2)

        val actual = await(runMongoRepository.getRun("dataset", 1, 2))

        assert(actual.contains(dataset1run2))
      }
    }

    "return None asynchronously" when {
      "there is no Run with the specified datasetName" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getRun("DATASET", 1, 1))

        assert(actual.isEmpty)
      }
      "there is no Run with the specified datasetVersion" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getRun("dataset", 2, 1))

        assert(actual.isEmpty)
      }
      "there is no Run with the specified runId" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getRun("dataset", 1, 2))

        assert(actual.isEmpty)
      }
      "the datasetName is null" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getRun(null, 1, 1))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getLatestRun" should {
    "return an Option of the Run with the highest runId for a specified dataset asynchronously" when {
      "there is a Run of the specified Dataset" in {
        val dataset1run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
        val dataset1run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 2)
        val dataset2run2 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 2, runId = 2)
        runFixture.add(dataset1run1, dataset1run2, dataset2run2)

        val actual = await(runMongoRepository.getLatestRun("dataset", 1))

        assert(actual.contains(dataset1run2))
      }
    }

    "return None asynchronously" when {
      "there is no Run with the specified datasetName" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getLatestRun("DATASET", 1))

        assert(actual.isEmpty)
      }
      "there is no Run with the specified datasetVersion" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getLatestRun("dataset", 2))

        assert(actual.isEmpty)
      }
      "the datasetName is null" in {
        setUpSimpleRun()

        val actual = await(runMongoRepository.getLatestRun(null, 1))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::create" should {
    "return the created Run" in {
      val run = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)

      await(runMongoRepository.create(run))
      val actual = await(runMongoRepository.getRun("dataset", 1, 1))

      assert(actual.contains(run))
    }
    "allow duplicate entries (this should be prohibited at the service layer)" in {
      val run = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)

      await(runMongoRepository.create(run))
      await(runMongoRepository.create(run))
      val actual = await(runMongoRepository.getRun("dataset", 1, 1))

      assert(await(runMongoRepository.count()) == 2)
      assert(actual.contains(run))
    }
  }

  "RunMongoRepository::appendCheckpoint" should {
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "add the supplied checkpoint to the end of the present checkpoints and return the updated Run" when {
      "there is a Run with the specified uniqueId" in {
        val checkpoint0 = RunFactory.getDummyCheckpoint(name = "checkpoint0")
        val measure = RunFactory.getDummyControlMeasure(checkpoints = List(checkpoint0))
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), controlMeasure = measure)
        runFixture.add(run)

        val checkpoint1 = RunFactory.getDummyCheckpoint(name = "checkpoint1")

        val actual = await(runMongoRepository.appendCheckpoint(uniqueId, checkpoint1))

        val expectedControlMeasure = run.controlMeasure.copy(checkpoints = List(checkpoint0, checkpoint1))
        val expected = run.copy(controlMeasure = expectedControlMeasure)
        assert(actual.contains(expected))
      }
    }

    "return None" when {
      "there is no Run with the specified uniqueId" in {
        val checkpoint = RunFactory.getDummyCheckpoint()

        val actual = await(runMongoRepository.appendCheckpoint(uniqueId, checkpoint))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::updateControlMeasure" should {
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "update the Run's ControlMeasure and return the updated Run" when {
      "there is a Run with the specified uniqueId" in {
        val originalMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Option("eeeeeeee-f9ac-46f8-9657-a09a4e3fb6e9"))
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), controlMeasure = originalMeasure)
        runFixture.add(run)

        val expectedMeasure = RunFactory.getDummyControlMeasure(runUniqueId = Option(uniqueId))

        val actual = await(runMongoRepository.updateControlMeasure(uniqueId, expectedMeasure))

        val expected = run.copy(controlMeasure = expectedMeasure)
        assert(actual.contains(expected))
      }
    }

    "return None" when {
      "there is no Run with the specified uniqueId" in {
        val controlMeasure = RunFactory.getDummyControlMeasure()

        val actual = await(runMongoRepository.updateControlMeasure(uniqueId, controlMeasure))

        assert(actual.isEmpty)
      }
    }
  }

  private def setUpSimpleRun(): Run = {
    val run = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
    runFixture.add(run)
    run
  }
}
