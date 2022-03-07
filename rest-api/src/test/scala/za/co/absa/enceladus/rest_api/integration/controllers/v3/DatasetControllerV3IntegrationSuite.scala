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
import org.scalatest.BeforeAndAfterAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.dataFrameFilter._
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.essentiality.Essentiality._
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, PropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, PropertyDefinitionFactory}
import za.co.absa.enceladus.model.versionedModel.VersionsList
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTest, BaseRestApiTestV3, toExpected}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(datasetFixture, propertyDefinitionFixture)

  s"POST $apiUrl" can {
    "return 201" when {
      "a Dataset is created" should {
        "return the created Dataset (with empty properties stripped)" in {
          val dataset = DatasetFactory.getDummyDataset("dummyDs",
            properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")))

          val response = sendPost[Dataset, Dataset](s"$apiUrl", bodyOpt = Some(dataset))
          assertCreated(response)

          val actual = response.getBody
          val expected = toExpected(dataset, actual).copy(properties = Some(Map("keyA" -> "valA", "keyB" -> "valB"))) // keyC stripped
          assert(actual == expected)
        }
      }
    }
  }

  s"PUT $apiUrl" can {
    "return 200" when {
      "a Schema with the given name and version is the latest that exists" should {
        "return the updated Schema (with empty properties stripped)" in {
          val datasetA1 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("init version"), properties = Some(Map("keyA" -> "valA")))
          datasetFixture.add(datasetA1)

          val exampleMappingCr = MappingConformanceRule(0,
            controlCheckpoint = true,
            mappingTable = "CurrencyMappingTable",
            mappingTableVersion = 9, //scalastyle:ignore magic.number
            attributeMappings = Map("InputValue" -> "STRING_VAL"),
            targetAttribute = "CCC",
            outputColumn = "ConformedCCC",
            isNullSafe = true,
            mappingTableFilter = Some(
              AndJoinedFilters(Set(
                OrJoinedFilters(Set(
                  EqualsFilter("column1", "soughtAfterValue"),
                  EqualsFilter("column1", "alternativeSoughtAfterValue")
                )),
                DiffersFilter("column2", "anotherValue"),
                NotFilter(IsNullFilter("col3"))
              ))
            ),
            overrideMappingTableOwnFilter = Some(true)
          )

          val datasetA2 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("updated"),
            properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")),
            conformance = List(exampleMappingCr)
          )

          val response = sendPut[Dataset, Dataset](s"$apiUrl", bodyOpt = Some(datasetA2))
          assertOk(response)

          // todo should be ok/no content and then actually no content -> run get again
          val actual = response.getBody
          val expectedDs = DatasetFactory.getDummyDataset(
              name = "datasetA",
              version = 2,
              description = Some("updated"),
              parent = Some(DatasetFactory.toParent(datasetA1)),
              properties = Some(Map("keyA" -> "valA", "keyB" -> "valB")),
              conformance = List(exampleMappingCr)
          )
          val expected = toExpected(expectedDs, actual)
          assert(actual == expected)
        }
      }
    }
  }

  s"GET $apiUrl/{name}" should {
    "return 200" when {
      "a Dataset with the given name exists - so it gives versions" in {
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA",
          version = 2,
          parent = Some(DatasetFactory.toParent(datasetV1)))
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[VersionsList](s"$apiUrl/datasetA")
        assertOk(response)
        assert(response.getBody == VersionsList("versions", Seq(1, 2)))
      }
    }

    "return 404" when {
      "a Dataset with the given name does not exist" in {
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/anotherDatasetName")
        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/export" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/2/export")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a correct Dataset version" should {
        "return the exported Dataset representation" in {
          val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
            properties = Some(Map("key1" -> "val1", "key2" -> "val2")))
          datasetFixture.add(dataset)
          val response = sendGet[String](s"$apiUrl/dataset/2/export")

          assertOk(response)

          val body = response.getBody
          assert(body ==
            """{"metadata":{"exportVersion":1},"item":{
              |"name":"dataset",
              |"hdfsPath":"/dummy/path",
              |"hdfsPublishPath":"/dummy/publish/path",
              |"schemaName":"dummySchema",
              |"schemaVersion":1,
              |"conformance":[],
              |"properties":{"key2":"val2","key1":"val1"}
              |}}""".stripMargin.replaceAll("[\\r\\n]", ""))
        }
      }
    }

  }

  // todo properties test for datasets or in general for any VersionedModel

}
