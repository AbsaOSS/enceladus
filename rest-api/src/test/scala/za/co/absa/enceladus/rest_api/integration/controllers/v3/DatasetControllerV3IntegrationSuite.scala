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
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.conformanceRule.{LiteralConformanceRule, MappingConformanceRule}
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

import scala.collection.JavaConverters._

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(datasetFixture, propertyDefinitionFixture)


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

  s"POST $apiUrl" can {
    "return 201" when {
      "a Dataset is created" should {
        "return the created Dataset (with empty properties stripped)" in {
          val dataset = DatasetFactory.getDummyDataset("dummyDs",
            properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")))

          val response = sendPost[Dataset, Dataset](apiUrl, bodyOpt = Some(dataset))
          assertCreated(response)
          val locationHeaders = response.getHeaders.get("location").asScala
          locationHeaders should have size 1
          val relativeLocation = stripBaseUrl(locationHeaders.head) // because locationHeader contains domain, port, etc.

          val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
          assertOk(response2)

          val actual = response2.getBody
          val expected = toExpected(dataset, actual).copy(properties = Some(Map("keyA" -> "valA", "keyB" -> "valB"))) // keyC stripped

          assert(actual == expected)
        }
      }
    }
    "the dataset is disabled (i.e. all version are disabled)" should {
      "create a new version of Dataset" in {
        val dataset1 = DatasetFactory.getDummyDataset("dummyDs", version = 1, disabled = true)
        val dataset2 = DatasetFactory.getDummyDataset("dummyDs", version = 2, disabled = true)
        datasetFixture.add(dataset1, dataset2)

        val dataset3 = DatasetFactory.getDummyDataset("dummyDs", version = 7) // version is ignored for create
        val response = sendPost[Dataset, String](apiUrl, bodyOpt = Some(dataset3))
        assertCreated(response)
        val locationHeaders = response.getHeaders.get("location").asScala
        locationHeaders should have size 1
        val relativeLocation = stripBaseUrl(locationHeaders.head) // because locationHeader contains domain, port, etc.

        val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(dataset3.copy(version = 3, parent = Some(DatasetFactory.toParent(dataset2))), actual)

        assert(actual == expected)
      }
    }
    // todo what to do if  "the last dataset version is disabled"
  }

  s"PUT $apiUrl" can {
    "return 200" when {
      "a Dataset with the given name and version is the latest that exists" should {
        "update the dataset (with empty properties stripped)" in {
          val datasetA1 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("init version"), properties = Some(Map("keyA" -> "valA")))
          val datasetA2 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("second version"), properties = Some(Map("keyA" -> "valA")), version = 2)

          datasetFixture.add(datasetA1, datasetA2)

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

          val datasetA3 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("updated"),
            properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")),
            conformance = List(exampleMappingCr),
            version = 2 // update references the last version
          )

          val response = sendPut[Dataset, String](s"$apiUrl/datasetA/2", bodyOpt = Some(datasetA3))
          response.getStatusCode shouldBe HttpStatus.NO_CONTENT

          val response2 = sendGet[Dataset](s"$apiUrl/datasetA/3") // next version
          assertOk(response2)
          val actual = response2.getBody
          val expected = toExpected(datasetA3.copy(version = 3, parent = Some(DatasetFactory.toParent(datasetA2)), properties = Some(Map("keyA" -> "valA", "keyB" -> "valB"))), actual) // blank property stripped
          assert(actual == expected)
        }
      }
    }

    s"GET $apiUrl/{name}/export" should {
      "return 404" when {
        "when the name does not exist" in {
          val response = sendGet[String](s"$apiUrl/notFoundDataset/export")
          assertNotFound(response)
        }
      }

      "return 200" when {
        "there is a correct Dataset" should {
          "return the exported Dataset representation for the latest version" in {
            val dataset1 = DatasetFactory.getDummyDataset(name = "dataset")
            val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2, description = Some("Hi, I am the latest version"),
              properties = Some(Map("key1" -> "val1", "key2" -> "val2")),
              conformance = List(LiteralConformanceRule(0, "outputCol1", controlCheckpoint = false, "litValue1"))
            )
            datasetFixture.add(dataset1, dataset2)
            val response = sendGet[String](s"$apiUrl/dataset/export")

            assertOk(response)

            val body = response.getBody
            assert(body ==
              """{"metadata":{"exportVersion":1},"item":{
                |"name":"dataset",
                |"description":"Hi, I am the latest version",
                |"hdfsPath":"/dummy/path",
                |"hdfsPublishPath":"/dummy/publish/path",
                |"schemaName":"dummySchema",
                |"schemaVersion":1,
                |"conformance":[{"_t":"LiteralConformanceRule","order":0,"outputColumn":"outputCol1","controlCheckpoint":false,"value":"litValue1"}],
                |"properties":{"key2":"val2","key1":"val1"}
                |}}""".stripMargin.replaceAll("[\\r\\n]", ""))
          }
        }
      }

    }

    "return 405" when {
      "a Dataset with the given name and version" should {
        "fail when version/name in the URL and payload is mismatched" in {
          val datasetA1 = DatasetFactory.getDummyDataset("datasetA", description = Some("init version"))
          datasetFixture.add(datasetA1)

          val response = sendPut[Dataset, String](s"$apiUrl/datasetA/7",
            bodyOpt = Some(DatasetFactory.getDummyDataset("datasetA", version = 5)))
          response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response.getBody should include("version mismatch: 7 != 5")

          val response2 = sendPut[Dataset, String](s"$apiUrl/datasetABC/4",
            bodyOpt = Some(DatasetFactory.getDummyDataset("datasetXYZ", version = 4)))
          response2.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response2.getBody should include("name mismatch: 'datasetABC' != 'datasetXYZ'")
        }
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
          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
            properties = Some(Map("key1" -> "val1", "key2" -> "val2")))
          val dataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3, description = Some("showing non-latest export"))
          datasetFixture.add(dataset2, dataset3)
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
