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

package za.co.absa.enceladus.menas.integration.repositories

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.integration.fixtures.{DatasetFixtureService, FixtureService, PropertyDefinitionFixtureService}
import za.co.absa.enceladus.menas.repositories.{DatasetMongoRepository, PropertyDefinitionMongoRepository}
import za.co.absa.enceladus.menas.services.StatisticsService
import za.co.absa.enceladus.model.properties.{PropertyDefinition, PropertyDefinitionDto}
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
  private val datasetMongoRepository: DatasetMongoRepository = null

  @Autowired
  private val propertyDefinitionRepository: PropertyDefinitionMongoRepository = null

  @Autowired
  private val statisticsService: StatisticsService = null

  override def fixtures: List[FixtureService[_]] = List(datasetFixture)

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
      Map("mandatoryString1"->"", "recommendedString1" -> "", "optionalString1"->""))),
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

  "StatisticsService" should {

    "return the properties with missing counts" when {
      "the specified datasets and propertie" in {
        datasetFixture.add(mockedDatasets: _*)
        propertyDefService.add(mockedPropertyDefinitions: _*)

        val actualStatistics = await(statisticsService.getPropertiesWithMissingCount())

        val expectedStatistics = Seq(
          PropertyDefinitionDto(name = "mandatoryString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
            datasetNumberMissing = 1),
          PropertyDefinitionDto(name = "mandatoryString2", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
            datasetNumberMissing = 2),
          PropertyDefinitionDto(name = "optionalEnumAb", propertyType = EnumPropertyType("optionA", "optionB"), essentiality = Optional,
            datasetNumberMissing = 4),
          PropertyDefinitionDto(name = "optionalString1", propertyType = StringPropertyType(), essentiality = Optional,
            datasetNumberMissing = 4),
          PropertyDefinitionDto(name = "recommendedString1", propertyType = StringPropertyType(), essentiality = Recommended,
            datasetNumberMissing = 3)
        )

        assert(actualStatistics == expectedStatistics)
      }
    }
  }
}
