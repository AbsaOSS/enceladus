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

package za.co.absa.enceladus.menas.integration.controllers

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.integration.fixtures._
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.{Essentiality, Mandatory, Optional, Recommended}
import za.co.absa.enceladus.model.properties.propertyType.{PropertyType, StringEnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, PropertyDefinitionFactory}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetApiIntegrationSuite extends BaseRestApiTest with BeforeAndAfterAll {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/dataset"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(datasetFixture, propertyDefinitionFixture)

  s"GET $apiUrl/detail/{name}/latestVersion" should {
    "return 200" when {
      "a Dataset with the given name exists" in {
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA",
          version = 2,
          parent = Some(DatasetFactory.toParent(datasetV1)))
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[String](s"$apiUrl/detail/datasetA/latestVersion")
        assertOk(response)
        assert("2" == response.getBody)
      }
    }

    "return 404" when {
      "a Dataset with the given name does not exist" in {
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/detail/anotherDatasetName/latestVersion")
        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/export/{name}/{version}" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/export/notFoundDataset/2")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a correct Dataset version" should {
        "return the exported PD representation" in {
          val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
            properties = Some(Map("key1" -> "val1", "key2" -> "val2")))
          datasetFixture.add(dataset)
          val response = sendGet[String](s"$apiUrl/exportItem/dataset/2")

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
              |"properties":[["key2","val2"],["key1","val1"]]
              |}}""".stripMargin.replaceAll("[\\r\\n]", ""))
        }
      }
    }

  }

  // Dataset specific:
  Seq(
    s"$apiUrl/{name}/{version}/properties",
    s"$apiUrl/{name}/properties"
  ).foreach { urlPattern =>
    s"GET $urlPattern" should {
      "return 404" when {
        "when the name+version does not exist" in {
          val response = sendGet[String](urlPattern
            .replace("{name}", "notFoundDataset")
            .replace("{version}", "123")) // version replacing has no effect for "latest version" urlPattern
          assertNotFound(response)
        }
      }

      "return 200" when {
        "there is a correct Dataset version" should {
          Seq(
            ("empty1", Some(Map.empty[String, String])),
            ("empty2", None),
            ("non-empty", Some(Map("key1" -> "val1", "key2" -> "val2")))
          ).foreach { case (propertiesCaseName, propertiesData) =>
            s"return dataset properties ($propertiesCaseName)" in {
              val datasetV1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
              val datasetV2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2, properties = propertiesData)
              datasetFixture.add(datasetV1, datasetV2)
              val response = sendGet[Map[String, String]](urlPattern
                .replace("{name}", "dataset")
                .replace("{version}", "2")) // version replacing has no effect for "latest version" urlPattern
              assertOk(response)

              val expectedProperties = propertiesData.getOrElse(Map.empty[String, String])
              val body = response.getBody
              assert(body == expectedProperties)
            }
          }
        }
      }
    }
  }

  s"PUT $apiUrl/{name}/properties" should {
    "201 Created with location = replace properties with a new version" when {
      "there is a correct Dataset version" in {
        val datasetV1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
        datasetFixture.add(datasetV1)
        val response1 = sendGet[Map[String, String]](s"$apiUrl/dataset/1/properties")
        assertOk(response1)

        val expectedProperties1 = Map.empty[String, String]
        val body1 = response1.getBody
        assert(body1 == expectedProperties1, "initially, there are no properties")

        val updatedProperties = Map("keyA" -> "valA", "keyB" -> "valB") // both put content & expected properties content
        val response2 = sendPut[Map[String, String], Dataset](s"$apiUrl/dataset/properties", bodyOpt = Some(updatedProperties))

        assertCreated(response2)
        val headers2 = response2.getHeaders
        val body2 = response2.getBody

        assert(headers2.getFirst("Location").contains("/api/dataset/dataset/2"))
        assert(body2.version == 2)
        assert(body2.propertiesAsMap == updatedProperties)
      }
    }
  }

    s"GET $apiUrl/{name}/{version}/properties/valid" should {
      "return 404" when {
        "when the dataset-by-name does not exist" in {
          val response = sendGet[String](s"$apiUrl/notExistingDataset/1/properties/valid")
          assertNotFound(response)
        }

        "when the dataset by name+version does not exist" in {
          val datasetAv1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
          datasetFixture.add(datasetAv1)

          val response = sendGet[String](s"$apiUrl/datasetA/123/properties/valid")
          assertNotFound(response)
        }

      }

      "return 200" when {
        "there is a correct Dataset name+version" should {
          s"return validated properties" in {
            def createPropDef(name: String, essentiality: Essentiality, propertyType: PropertyType): PropertyDefinition =
              PropertyDefinitionFactory.getDummyPropertyDefinition(name, essentiality = essentiality, propertyType = propertyType)

            val propDefS1 = createPropDef("mandatoryField1", Mandatory(), StringPropertyType("default1"))
            val propDefS2 = propDefS1.copy(name = "mandatoryField2")
            val propDefE1 = createPropDef("enumField1", Optional(), StringEnumPropertyType("optionA", "optionB"))
            val propDefE2 = createPropDef("enumField2", Recommended(), StringEnumPropertyType("optionC", "optionD"))
            propertyDefinitionFixture.add(propDefE1, propDefE2, propDefS1, propDefS2)

            val datasetAv2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2,
              properties = Some(Map(
                "mandatoryField1" -> "its value", // mandatoryField2 missing
                "enumField1" -> "invalidOption", // enumField2 is just recommended
                "nonAccountedField" -> "randomVal"
              )))
            // showing that the version # is respected, even for the non-latest
            val datasetAv3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
            datasetFixture.add(datasetAv2, datasetAv3)

            val response = sendGet[Validation](s"$apiUrl/datasetA/2/properties/valid")
            assertOk(response)

            val expectedValidation = Validation(Map(
              "mandatoryField2" -> List("Dataset property 'mandatoryField2' is mandatory, but does not exist!"),
              "enumField1" -> List("Value invalidOption of key 'enumField1' does not conform to the property type of StringEnumPropertyType(Set(optionA, optionB),optionA)."),
              "nonAccountedField" -> List("There is no property definition for key 'nonAccountedField'.")
            ))
            val body = response.getBody
            assert(body == expectedValidation)
          }
        }
      }

  }

}
