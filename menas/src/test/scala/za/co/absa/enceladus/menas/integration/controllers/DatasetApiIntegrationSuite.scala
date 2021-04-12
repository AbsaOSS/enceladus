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
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.essentiality.Essentiality._
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, PropertyType, StringPropertyType}
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
              |"properties":{"key2":"val2","key1":"val1"}
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

  Seq(
    s"$apiUrl/{name}/{version}/properties?putIntoInfoFile=true" -> Set("infoField1"),
    s"$apiUrl/{name}/properties?putIntoInfoFile=true" -> Set("infoField1"),
    s"$apiUrl/{name}/properties?putIntoInfoFile=false" -> Set("field1"),
    s"$apiUrl/{name}/properties?putIntoInfoFile=bogus" -> Set("infoField1", "field1") // but not "extraUnwantedField1"
  ).foreach { case (urlPattern, expectedKeys) =>
    s"GET $urlPattern" should {
      "return 200" when {
        "there is a correct Dataset version" should {
          s"return dataset properties (filtered)" in {
            def createPropDef(name: String, putIntoInfoFile: Boolean, disabled: Boolean): PropertyDefinition =
              PropertyDefinitionFactory.getDummyPropertyDefinition(name, putIntoInfoFile = putIntoInfoFile, disabled = disabled)

            val propDef1 = createPropDef("field1", putIntoInfoFile = false, disabled = false)
            val propDef2 = createPropDef(name = "infoField1", putIntoInfoFile = true, disabled = false)
            val propDef2a = createPropDef(name = "infoField2", putIntoInfoFile = true, disabled = false)
            propertyDefinitionFixture.add(propDef1, propDef2, propDef2a)

            val datasetAv2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2)
            val datasetAv3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3,
              properties = Some(Map(
                "field1" -> "someValueA",
                "infoField1" -> "someValueB",
                // infoField2 is missing
                "extraUnwantedField1" -> "whatever"
              ))
            )
            datasetFixture.add(datasetAv2, datasetAv3)

            val response = sendGet[Map[String, String]](urlPattern
              .replace("{name}", "datasetA")
              .replace("{version}", "3") // version is not used for the latest-url
            )
            assertOk(response)

            val body = response.getBody
            assert(body.keySet == expectedKeys)
          }
        }
      }
    }
  }

  s"PUT $apiUrl/{name}/properties" should {
    "201 Created with location " when {
      Seq(
        ("non-empty properties map", """{"keyA":"valA","keyB":"valB"}""", Some(Map("keyA" -> "valA", "keyB" -> "valB"))),
        ("empty properties map", "{}", Some(Map.empty)),
        ("no properties at all", "", None) // this is backwards compatible option with the pre-properties era versions
      ).foreach {case (testCaseName, payload, expectedPropertiesSet) =>
        s"properties are replaced with a new version ($testCaseName)" in {
          val datasetV1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
          datasetFixture.add(datasetV1)
          val response1 = sendGet[Map[String, String]](s"$apiUrl/dataset/1/properties")
          assertOk(response1)

          val expectedProperties1 = Map.empty[String, String]
          val body1 = response1.getBody
          assert(body1 == expectedProperties1, "initially, there are no properties")

          val response2 = sendPut[String, Dataset](s"$apiUrl/dataset/properties", bodyOpt = Some(payload))

          assertCreated(response2)
          val headers2 = response2.getHeaders
          val body2 = response2.getBody

          assert(headers2.getFirst("Location").contains("/api/dataset/dataset/2"))
          assert(body2.version == 2)
          assert(body2.properties == expectedPropertiesSet)
        }
      }
    }
  }

  { // properties validation
    def createPropDef(name: String, essentiality: Essentiality, propertyType: PropertyType): PropertyDefinition =
      PropertyDefinitionFactory.getDummyPropertyDefinition(name, essentiality = essentiality, propertyType = propertyType)

    val propDefs = Seq(
      createPropDef("mandatoryField1", Mandatory(false), StringPropertyType("default1")),
      createPropDef("mandatoryField2", Mandatory(false), StringPropertyType("default1")),
      createPropDef("mandatoryField3", Mandatory(true), StringPropertyType("default1")),
      createPropDef("enumField1", Optional, EnumPropertyType("optionA", "optionB")),
      createPropDef("enumField2", Recommended, EnumPropertyType("optionC", "optionD"))
    )

    val properties = Map(
      "mandatoryField1" -> "its value", // mandatoryField2 missing
      "enumField1" -> "invalidOption", // enumField2 is just recommended
      "nonAccountedField" -> "randomVal"
    )

    val expectedValidationForRun = Validation(Map(
      "mandatoryField2" -> List("Dataset property 'mandatoryField2' is mandatory, but does not exist!"),
      "enumField1" -> List("Value 'invalidOption' is not one of the allowed values (optionA, optionB)."),
      "nonAccountedField" -> List("There is no property definition for key 'nonAccountedField'.")
    ))

    val expectedValidationStrictest = expectedValidationForRun
      .withError("mandatoryField3", "Dataset property 'mandatoryField3' is mandatory, but does not exist!")

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

      Seq(
        ("", expectedValidationStrictest),
        ("?forRun=False", expectedValidationStrictest),
        ("?forRun=TRUE", expectedValidationForRun)
      ).foreach { case (queryString, expectedValidation) =>
        "return 200" when {
          s"there is a correct Dataset name+version with query params: $queryString" should {
            s"return validated properties" in {
              propertyDefinitionFixture.add(propDefs: _*)
              val datasetAv2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, properties = Some(properties))
              // showing that the version # is respected, even for the non-latest
              val datasetAv3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
              datasetFixture.add(datasetAv2, datasetAv3)

              val response = sendGet[Validation](s"$apiUrl/datasetA/2/properties/valid$queryString")
              assertOk(response)

              val body = response.getBody
              assert(body == expectedValidation)
            }
          }
        }
      }

      "return 400" when {
        "when using wrong query string" in {
          val response = sendGet[String](s"$apiUrl/datasetX/1/properties/valid?forRun=what")
          assertBadRequest(response)
          val actual = response.getBody
          assertResult(actual)(s"Unrecognized value 'what' for parameter `forRun`")
        }
      }
    }

    Seq(
      (s"$apiUrl/datasetA/2?validateProperties=Strictest", Some(expectedValidationStrictest)),
      (s"$apiUrl/datasetA/2?validateProperties=ForRun", Some(expectedValidationForRun)),
      (s"$apiUrl/datasetA/2?validateProperties=NoValidation", None),
      (s"$apiUrl/datasetA/2", None)
    ).foreach { case (url, expectedPropertiesValidation) =>

      s"GET $url" should {
        "return 404" when {
          "when the dataset-by-name does not exist" in {
            val response = sendGet[String](url)
            assertNotFound(response)
          }

          "when the dataset by name+version does not exist" in {
            val datasetAv1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
            datasetFixture.add(datasetAv1)

            val response = sendGet[String](url) // v2 does not exist
            assertNotFound(response)
          }
        }

        "return 200" when {
          "there is a correct Dataset name+version" should {
            s"return dataset with validated properties" in {
              propertyDefinitionFixture.add(propDefs: _*)

              val datasetAv2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, properties = Some(properties))
              // showing that the version # is respected, even for the non-latest
              val datasetAv3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
              datasetFixture.add(datasetAv2, datasetAv3)

              val response = sendGet[Dataset](url)
              assertOk(response)

              val actual = response.getBody
              val expected = toExpected(datasetAv2, actual).copy(propertiesValidation = expectedPropertiesValidation) // will (not) miss expectedValidation
              assert(actual == expected)
            }
          }
        }
      }
    }

    val wrongValidateProperties = "foo"
    val urlWithWrongQueryParams = s"$apiUrl/datasetX/1?validateProperties=$wrongValidateProperties"
    s"GET $urlWithWrongQueryParams" should {
      "return 400" when {
        "when the query parameter has an unrecognized value" in {
          val response = sendGet[String](urlWithWrongQueryParams)
          assertBadRequest(response)
          val actual = response.getBody
          assertResult(actual)(s"Unrecognized value '$wrongValidateProperties' for parameter `validateProperties`")
        }
      }
    }
  }

}
