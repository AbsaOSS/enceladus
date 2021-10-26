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

package za.co.absa.enceladus.rest_api.integration.repositories

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.rest_api.integration.fixtures.{DatasetFixtureService, FixtureService, PropertyDefinitionFixtureService}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, PropertyDefinitionMongoRepository}
import za.co.absa.enceladus.rest_api.services.StatisticsService
import za.co.absa.enceladus.model.properties.{PropertyDefinition, PropertyDefinitionStats}
import za.co.absa.enceladus.model.properties.essentiality.Essentiality.{Mandatory, Optional, Recommended}
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.DatasetFactory

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class StatisticsIntegrationSuite extends BaseRepositoryTest {
  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val propertyDefService: PropertyDefinitionFixtureService = null

  @Autowired
  private val statisticsService: StatisticsService = null

  override def fixtures: List[FixtureService[_]] = List(datasetFixture)

  val mockedPropertyDefinitions = Seq(
    PropertyDefinition(name = "mandatoryString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
      userCreated = "", userUpdated = ""),
    PropertyDefinition(name = "mandatoryString2", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
      userCreated = "", userUpdated = ""),
    PropertyDefinition(name = "recommendedString1", propertyType = StringPropertyType(), essentiality = Recommended,
      userCreated = "", userUpdated = ""),
    PropertyDefinition(name = "optionalString1", propertyType = StringPropertyType(), essentiality = Optional,
      userCreated = "", userUpdated = ""),
    PropertyDefinition(name = "mandatoryDisabledString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
      disabled = true, userCreated = "", userUpdated = ""),
    PropertyDefinition(name = "optionalEnumAb", propertyType = EnumPropertyType("optionA", "optionB"), essentiality = Optional,
      userCreated = "", userUpdated = "")
  )

  val mockedDatasets = Seq(
    DatasetFactory.getDummyDataset(name = "dataset1", version = 1, properties = Some(
      Map())),
    DatasetFactory.getDummyDataset(name = "dataset1", version = 2, properties = Some(
      Map("mandatoryString1"->""))),
    DatasetFactory.getDummyDataset(name = "dataset1", version = 3, properties = Some(
      Map("mandatoryString1"->"", "mandatoryString2"->"3", "optionalEnumAb" -> "optionA"))),
    DatasetFactory.getDummyDataset(name = "dataset2", version = 1, properties = Some(
      Map("recommendedString1" -> "", "optionalString1"->""))),
    DatasetFactory.getDummyDataset(name = "dataset2", version = 2, properties = Some(
      Map("mandatoryString1"->"", "recommendedString1" -> ""))),
    DatasetFactory.getDummyDataset(name = "dataset3", version = 1, properties = Some(
      Map("mandatoryString1"->""))),
    DatasetFactory.getDummyDataset(name = "dataset3", version = 2, properties = Some(
      Map("mandatoryString1"->"","mandatoryString2"->"3"))),
    DatasetFactory.getDummyDataset(name = "dataset4", version = 1, properties = Some(
      Map("mandatoryString1"->"", "mandatoryString2"->"3", "recommendedString1" -> "", "optionalString1"->"",
        "mandatoryDisabledString1" -> "", "optionalEnumAb" -> "optionA"))),
    DatasetFactory.getDummyDataset(name = "dataset5", version = 1, properties = Some(Map())),
    DatasetFactory.getDummyDataset(name = "dataset6", version = 1, properties = Some(Map(
      "mandatoryString1"->"", "mandatoryString2"->"3", "recommendedString1" -> "")))
  )

  "StatisticsService" should {

    "return the properties with missing counts" when {
      "the specified datasets and properties" in {
        datasetFixture.add(mockedDatasets: _*)
        propertyDefService.add(mockedPropertyDefinitions: _*)

        val actualStatistics = await(statisticsService.getPropertiesWithMissingCount()).sortBy(_.name)

        val expectedStatistics = Seq(
          PropertyDefinitionStats(name = "mandatoryString1", essentiality = Mandatory(allowRun = false),
            missingInDatasetsCount = 1), // missing in dataset5
          PropertyDefinitionStats(name = "mandatoryString2", essentiality = Mandatory(allowRun = false),
            missingInDatasetsCount = 2), // missing in dataset2,5
          PropertyDefinitionStats(name = "optionalEnumAb", essentiality = Optional,
            missingInDatasetsCount = 4), // missing in dataset2,3,5,6
          PropertyDefinitionStats(name = "optionalString1", essentiality = Optional,
            missingInDatasetsCount = 5), // missing in dataset1,2,3,5,6
          PropertyDefinitionStats(name = "recommendedString1", essentiality = Recommended,
            missingInDatasetsCount = 3) // missing in dataset1,3,5
        )

        assert(actualStatistics == expectedStatistics)
      }
    }
  }
}
