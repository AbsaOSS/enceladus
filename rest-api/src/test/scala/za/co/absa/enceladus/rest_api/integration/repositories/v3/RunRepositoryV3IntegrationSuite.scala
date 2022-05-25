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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.atum.model.{RunState, RunStatus}
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.model.test.factories.RunFactory
import za.co.absa.enceladus.rest_api.integration.fixtures.{FixtureService, RunFixtureService}
import za.co.absa.enceladus.rest_api.integration.repositories.BaseRepositoryTest
import za.co.absa.enceladus.rest_api.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary}
import za.co.absa.enceladus.rest_api.repositories.RunMongoRepository
import za.co.absa.enceladus.rest_api.repositories.v3.RunMongoRepositoryV3
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.UUID

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class RunRepositoryV3IntegrationSuite extends BaseRepositoryTest {

  import za.co.absa.enceladus.rest_api.integration.RunImplicits.RunExtensions

  @Autowired
  private val runFixture: RunFixtureService = null

  @Autowired
  private val runMongoRepository: RunMongoRepositoryV3 = null

  override def fixtures: List[FixtureService[_]] = List(runFixture)

  private val today = LocalDate.now(ZoneId.of(TimeZoneNormalizer.timeZone)).format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))

  "RunMongoRepository::getLatestOfEachRunSummary" should {
    "return only the latest runs" in {
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
  }

  // todo add other tests for different params

}
