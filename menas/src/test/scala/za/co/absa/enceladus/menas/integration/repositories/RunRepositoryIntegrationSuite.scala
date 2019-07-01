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

package za.co.absa.enceladus.menas.integration.repositories

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.atum.model.{RunState, RunStatus}
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.menas.factories.RunFactory
import za.co.absa.enceladus.menas.integration.fixtures.RunFixtureService
import za.co.absa.enceladus.menas.models.RunSummary
import za.co.absa.enceladus.menas.repositories.RunMongoRepository
import java.time.LocalDate
import java.time.format.DateTimeFormatter

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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

  private val today = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))

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
        val dataset3run1 = RunFactory.getDummyRun(dataset = "dataset3", runId = 1, startDateTime = "29-01-2019 13:01:12 +0200")
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

  "RunMongoRepository::getAllSummaries" should {
    "return all RunSummaries" when {
      "there are Runs in the database" in {
        val startDateTime1 = "29-01-2019 13:01:12 +0200"
        val startDateTime2 = "29-03-2019 13:01:12 +0200"

        val dataset1v1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1,
          runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())), startDateTime = startDateTime1)
        val dataset1v1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2,
          runStatus = RunStatus(RunState.running, None), startDateTime = startDateTime2)
        val dataset1v2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1,
          runStatus = RunStatus(RunState.stageSucceeded, None), startDateTime = startDateTime1)
        val dataset2v1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1,runId = 1,
          runStatus = RunStatus(RunState.allSucceeded, None), startDateTime = startDateTime2)
        runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

        val actual = await(runMongoRepository.getAllSummaries())

        val dataset1v1run1summary = RunSummary(datasetName = "dataset1", datasetVersion = 1, runId = 1,
          status = "failed", startDateTime = startDateTime1)
        val dataset1v1run2summary = RunSummary(datasetName = "dataset1", datasetVersion = 1, runId = 2,
          status = "running", startDateTime = startDateTime2)
        val dataset1v2run1summary = RunSummary(datasetName = "dataset1", datasetVersion = 2, runId = 1,
          status = "stageSucceeded", startDateTime = startDateTime1)
        val dataset2v1run1summary = RunSummary(datasetName = "dataset2", datasetVersion = 1, runId = 1,
          status = "allSucceeded", startDateTime = startDateTime2)
        val expected = List(dataset1v1run1summary, dataset1v1run2summary, dataset1v2run1summary, dataset2v1run1summary)

        assert(actual == expected)
      }
    }

    "return an empty collection asynchronously" when {
      "there are no Runs in the database" in {
        val actual = await(runMongoRepository.getAllSummaries())

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getSummariesByDatasetName" should {
    val startDateTime1 = "29-01-2019 13:01:12 +0200"
    val startDateTime2 = "29-03-2019 13:01:12 +0200"

    val queriedDatasetName = "dataset1"
    val wrongDatasetName = "dataset2"

    "return only the latest run for each dataset on that startDate asynchronously" when {
      "there are Runs with the specified Dataset Name" in {
        val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 1,
          runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())), startDateTime = startDateTime1)
        val dataset1v1run2 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 1, runId = 2,
          runStatus = RunStatus(RunState.running, None), startDateTime = startDateTime2)
        val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = 2, runId = 1,
          runStatus = RunStatus(RunState.stageSucceeded, None), startDateTime = startDateTime1)

        val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 1,runId = 1,
          runStatus = RunStatus(RunState.allSucceeded, None), startDateTime = startDateTime2)
        runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

        val actual = await(runMongoRepository.getSummariesByDatasetName(queriedDatasetName))

        val dataset1v1run1summary = RunSummary(datasetName = queriedDatasetName, datasetVersion = 1, runId = 1,
          status = "failed", startDateTime = startDateTime1)
        val dataset1v1run2summary = RunSummary(datasetName = queriedDatasetName, datasetVersion = 1, runId = 2,
          status = "running", startDateTime = startDateTime2)
        val dataset1v2run1summary = RunSummary(datasetName = queriedDatasetName, datasetVersion = 2, runId = 1,
          status = "stageSucceeded", startDateTime = startDateTime1)
        val expected = List(dataset1v1run1summary, dataset1v1run2summary, dataset1v2run1summary)

        assert(actual == expected)
      }
    }

    "return an empty collection asynchronously" when {
      "there are no Runs with the specified Dataset Name" in {
        val run = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = 1,runId = 1,
          runStatus = RunStatus(RunState.allSucceeded, None), startDateTime = startDateTime2)
        runFixture.add(run)

        val actual = await(runMongoRepository.getByStartDate(queriedDatasetName))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getSummariesByDatasetNameAndVersion" should {
    val startDateTime1 = "29-01-2019 13:01:12 +0200"
    val startDateTime2 = "29-03-2019 13:01:12 +0200"

    val queriedDatasetName = "dataset1"
    val wrongDatasetName = "dataset2"

    val queriedDatasetVersion = 1
    val wrongDatasetVersion = 2

    "return only the latest run for each dataset on that startDate asynchronously" when {
      "there are Runs with the specified Dataset Name and version" in {
        val dataset1v1run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = queriedDatasetVersion, runId = 1,
          runStatus = RunStatus(RunState.failed, Option(RunFactory.getDummyRunError())), startDateTime = startDateTime1)
        val dataset1v1run2 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = queriedDatasetVersion, runId = 2,
          runStatus = RunStatus(RunState.running, None), startDateTime = startDateTime2)

        val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = wrongDatasetVersion, runId = 1,
          runStatus = RunStatus(RunState.stageSucceeded, None), startDateTime = startDateTime1)
        val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = queriedDatasetVersion,runId = 1,
          runStatus = RunStatus(RunState.allSucceeded, None), startDateTime = startDateTime2)
        runFixture.add(dataset1v1run1, dataset1v1run2, dataset1v2run1, dataset2v1run1)

        val actual = await(runMongoRepository.getSummariesByDatasetNameAndVersion(queriedDatasetName, queriedDatasetVersion))

        val dataset1v1run1summary = RunSummary(datasetName = queriedDatasetName, datasetVersion = 1, runId = 1,
          status = "failed", startDateTime = startDateTime1)
        val dataset1v1run2summary = RunSummary(datasetName = queriedDatasetName, datasetVersion = 1, runId = 2,
          status = "running", startDateTime = startDateTime2)
        val expected = List(dataset1v1run1summary, dataset1v1run2summary)

        assert(actual == expected)
      }
    }

    "return an empty collection asynchronously" when {
      "there are no Runs with the specified Dataset Name and Version" in {
        val dataset1v2run1 = RunFactory.getDummyRun(dataset = queriedDatasetName, datasetVersion = wrongDatasetVersion, runId = 1,
          runStatus = RunStatus(RunState.stageSucceeded, None), startDateTime = startDateTime1)
        val dataset2v1run1 = RunFactory.getDummyRun(dataset = wrongDatasetName, datasetVersion = queriedDatasetVersion,runId = 1,
          runStatus = RunStatus(RunState.allSucceeded, None), startDateTime = startDateTime2)
        runFixture.add(dataset1v2run1, dataset2v1run1)

        val actual = await(runMongoRepository.getSummariesByDatasetNameAndVersion(queriedDatasetName, queriedDatasetVersion))

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
    "store the specified Run in the database" in {
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

  "RunMongoRepository::updateSplineReference" should {
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "update the Run's SplineReference and return the updated Run" when {
      "there is a Run with the specified uniqueId" in {
        val originalSplineRef = RunFactory.getDummySplineReference(sparkApplicationId = null)
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), splineRef = originalSplineRef)
        runFixture.add(run)

        val expectedSplineRef = RunFactory.getDummySplineReference(sparkApplicationId = "application_1512977199009_0007")

        val actual = await(runMongoRepository.updateSplineReference(uniqueId, expectedSplineRef))

        val expected = run.copy(splineRef = expectedSplineRef)
        assert(actual.contains(expected))
      }
    }

    "return None" when {
      "there is no Run with the specified uniqueId" in {
        val splineReference = RunFactory.getDummySplineReference()

        val actual = await(runMongoRepository.updateSplineReference(uniqueId, splineReference))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::updateRunStatus" should {
    val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

    "update the Run's RunStatus and return the updated Run" when {
      "there is a Run with the specified uniqueId" in {
        val originalStatus = RunFactory.getDummyRunStatus(runState = RunState.running)
        val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId), runStatus = originalStatus)
        runFixture.add(run)

        val expectedStatus = RunFactory.getDummyRunStatus(runState = RunState.allSucceeded)

        val actual = await(runMongoRepository.updateRunStatus(uniqueId, expectedStatus))

        val expected = run.copy(runStatus = expectedStatus)
        assert(actual.contains(expected))
      }
    }

    "return None" when {
      "there is no Run with the specified uniqueId" in {
        val runStatus = RunFactory.getDummyRunStatus()

        val actual = await(runMongoRepository.updateRunStatus(uniqueId, runStatus))

        assert(actual.isEmpty)
      }
    }
  }

  "RunMongoRepository::getTodaysRuns" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysRuns()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysRuns()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1, startDateTime = today)
        val run2 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = today)
        runFixture.add(run1, run2)
        assert(await(runMongoRepository.getTodaysRuns()) == 2)
      }
    }
  }

  "RunMongoRepository::getTodaysSuccessfulRuns" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysSuccessfulRuns()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysSuccessfulRuns()) == 0)
      }

      "there are no successful runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.failed))
        runFixture.add(run1)
        assert(await(runMongoRepository.getTodaysSuccessfulRuns()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are successful runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1, startDateTime = today)
        val run2 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1, startDateTime = today)
        val run3 = RunFactory.getDummyRun(dataset = "dataset3",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.running))
        runFixture.add(run1, run2, run3)
        assert(await(runMongoRepository.getTodaysSuccessfulRuns()) == 2)
      }
    }
  }

  "RunMongoRepository::getTodaysFailedRuns" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysFailedRuns()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysFailedRuns()) == 0)
      }

      "there are no failed runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.running))
        runFixture.add(run1)
        assert(await(runMongoRepository.getTodaysFailedRuns()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are failed runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.failed))
        val run2 = RunFactory.getDummyRun(dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.failed))
        val run3 = RunFactory.getDummyRun(dataset = "dataset3",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today)
        runFixture.add(run1, run2, run3)
        assert(await(runMongoRepository.getTodaysFailedRuns()) == 2)
      }
    }
  }

  "RunMongoRepository::getTodaysStdSuccessRuns" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysStdSuccessRuns()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysStdSuccessRuns()) == 0)
      }

      "there are no stdSuccessful runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1, startDateTime = today)
        runFixture.add(run1)
        assert(await(runMongoRepository.getTodaysStdSuccessRuns()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are stdSuccessful runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.stageSucceeded))
        val run2 = RunFactory.getDummyRun(dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.stageSucceeded))
        val run3 = RunFactory.getDummyRun(dataset = "dataset3",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today)
        runFixture.add(run1, run2, run3)
        assert(await(runMongoRepository.getTodaysStdSuccessRuns()) == 2)
      }
    }
  }

  "RunMongoRepository::getTodaysRunningRuns" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysRunningRuns()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysRunningRuns()) == 0)
      }

      "there are no running runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1, startDateTime = today)
        runFixture.add(run1)
        assert(await(runMongoRepository.getTodaysRunningRuns()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are running runs from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.running))
        val run2 = RunFactory.getDummyRun(dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            runStatus = RunFactory.getDummyRunStatus(RunState.running))
        val run3 = RunFactory.getDummyRun(dataset = "dataset3",
            datasetVersion = 1,
            runId = 1, 
            startDateTime = today)
        runFixture.add(run1, run2, run3)
        assert(await(runMongoRepository.getTodaysRunningRuns()) == 2)
      }
    }
  }

  "RunMongoRepository::getTodaysSuccessWithErrors" should {
    "return 0" when {
      "there are no runs" in {
        assert(await(runMongoRepository.getTodaysSuccessWithErrors()) == 0)
      }

      "there are no runs from today" in {
        setUpSimpleRun()
        assert(await(runMongoRepository.getTodaysSuccessWithErrors()) == 0)
      }

      "there are no successfull runs with errors from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1, startDateTime = today)
        runFixture.add(run1)
        assert(await(runMongoRepository.getTodaysSuccessWithErrors()) == 0)
      }
    }
    "return number of runs with today's date" when {
      "there are successful runs with errors from today" in {
        setUpSimpleRun()
        val run1 = RunFactory.getDummyRun(dataset = "dataset",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today,
            controlMeasure = RunFactory.getDummyControlMeasure(
                metadata = RunFactory.getDummyMetadata(
                    additionalInfo = Map("std_errors_count" -> "5"))))
        val run2 = RunFactory.getDummyRun(dataset = "dataset2",
            datasetVersion = 1,
            runId = 1,
            startDateTime = today)
        runFixture.add(run1, run2)
        assert(await(runMongoRepository.getTodaysSuccessWithErrors()) == 1)
      }
    }
  }

  private def setUpSimpleRun(): Run = {
    val run = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
    runFixture.add(run)
    run
  }

}
