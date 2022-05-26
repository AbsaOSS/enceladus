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

package za.co.absa.enceladus.rest_api.integration.repositories.v3

import com.mongodb.MongoWriteException
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.atum.model.{RunState, RunStatus}
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.model.test.factories.RunFactory
import za.co.absa.enceladus.rest_api.integration.fixtures.{FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.integration.repositories.BaseRepositoryTest
import za.co.absa.enceladus.rest_api.repositories.RunMongoRepository
import za.co.absa.enceladus.rest_api.repositories.v3.RunMongoRepositoryV3
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.UUID

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class RunRepositoryV3IntegrationSuite extends BaseRepositoryTest with Matchers {

  import za.co.absa.enceladus.rest_api.integration.RunImplicits.RunExtensions

  @Autowired
  private val runFixture: RunFixtureService = null

  @Autowired
  private val runMongoRepository: RunMongoRepositoryV3 = null

  override def fixtures: List[FixtureService[_]] = List(runFixture)

  private val today = LocalDate.now(ZoneId.of(TimeZoneNormalizer.timeZone)).format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))

  "RunMongoRepository::getLatestOfEachRunSummary" should {
    "return only the latest RunSummaries" in {
      val dataset1ver1run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 1)
      val dataset1ver1run2 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 1, runId = 2)
      runFixture.add(dataset1ver1run1, dataset1ver1run2)
      val dataset1ver2run1 = RunFactory.getDummyRun(dataset = "dataset1", datasetVersion = 2, runId = 1)
      runFixture.add(dataset1ver2run1)
      val dataset2ver1run1 = RunFactory.getDummyRun(dataset = "dataset2", datasetVersion = 1, runId = 1)
      runFixture.add(dataset2ver1run1)

      val actual = await(runMongoRepository.getLatestOfEachRunSummary())

      val expected = List(dataset1ver1run2, dataset1ver2run1, dataset2ver1run1).map(_.toSummary)
      assert(actual == expected)
    }

    "return only RunSummaries on startDate or later" in {
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

      val actual = await(runMongoRepository.getLatestOfEachRunSummary(startDate = Some("20-05-2022")))
      val expected = List(dataset1ver1run2, dataset1ver2run3, dataset3ver1run1).map(_.toSummary)
      assert(actual == expected)
    }

    "return only RunSummaries with the correct uniqueId" in {
      val r1id = UUID.randomUUID().toString
      val run1 = RunFactory.getDummyRun(dataset = "dataset1", runId = 1, uniqueId = Some(r1id))
      val run2 = RunFactory.getDummyRun(dataset = "dataset1", runId = 2, uniqueId = Some(UUID.randomUUID().toString)) // some other id
      val run3 = RunFactory.getDummyRun(dataset = "datasetX", uniqueId = None)

      runFixture.add(run1, run2, run3)

      val actual = await(runMongoRepository.getLatestOfEachRunSummary(uniqueId = Some(r1id)))
      val expected = List(run1).map(_.toSummary)
      assert(actual == expected)
    }

    "return only RunSummaries sparkAppId reference" in {
      val sampleAppId1 = "application_1578585424019_0008" // YARN
      val sampleAppId2 = "local-1433865536131" // local
      val sampleAppId3 = "driver-20170926223339-0001" // MESOS

      val run1 = prepareRunWithAppIds(Some(sampleAppId1), None, runId = 1) // std app_id only
      val run2 = prepareRunWithAppIds(Some(sampleAppId2), Some(sampleAppId3), runId = 2) // both std and cnfrm app_ids
      runFixture.add(run1, run2)

      // get summary of run1 by std app_id
      await(runMongoRepository.getLatestOfEachRunSummary(sparkAppId = Some(sampleAppId1))) shouldBe Seq(run1.toSummary)

      // get summary of run2 by std app_id
      await(runMongoRepository.getLatestOfEachRunSummary(sparkAppId = Some(sampleAppId2))) shouldBe Seq(run2.toSummary)

      // get summary of run2 by conform app_id
      await(runMongoRepository.getLatestOfEachRunSummary(sparkAppId = Some(sampleAppId3))) shouldBe Seq(run2.toSummary)

      // get nothing by a different sparkAppId
      await(runMongoRepository.getLatestOfEachRunSummary(sparkAppId = Some("application_1653565036000_12345"))) shouldBe Seq.empty
    }
  }


  private def prepareRunWithAppIds(stdAppId: Option[String], confAppId: Option[String], runId: Int = 1): Run = {

    val additionalInfo: Map[String, String] =
      stdAppId.map(id => Map("std_application_id" -> id)).getOrElse(Map.empty) ++
        confAppId.map(id => Map("conform_application_id" -> id)).getOrElse(Map.empty)

    val metadata = RunFactory.getDummyMetadata(additionalInfo = additionalInfo)
    val controlMeasure = RunFactory.getDummyControlMeasure(metadata = metadata)
    RunFactory.getDummyRun(runId = runId, controlMeasure = controlMeasure)

  }
}
