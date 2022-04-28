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
import za.co.absa.enceladus.model.{UsedIn, Validation}
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, PropertyDefinitionFactory}
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.models.rest.DisabledPayload

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class PropertyDefinitionControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  private val apiUrl = "/property-definitions/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(propertyDefinitionFixture, datasetFixture)


  private def minimalPdCreatePayload(name: String, suggestedValue: Option[String]) = {
    val suggestedValuePart = suggestedValue match {
      case Some(actualSuggestedValue) => s""","suggestedValue": "$actualSuggestedValue""""
      case _ => ""
    }

    s"""{"name": "$name","propertyType": {"_t": "StringPropertyType"$suggestedValuePart}}"""
  }

  private def invalidPayload(name: String) =
    s"""{
       |"name": "$name",
       |"propertyType": {
       |    "_t": "EnumPropertyType",
       |    "allowedValues": ["a", "b"],
       |    "suggestedValue": "invalidOptionC"
       |}
       |}""".stripMargin

  s"POST $apiUrl" can {
    "return 201" when {
      "PropertyDefinition is created" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
        val response = sendPostByAdmin[PropertyDefinition, Validation](apiUrl, bodyOpt = Some(propertyDefinition))
        assertCreated(response)

        val locationHeader = response.getHeaders.getFirst("location")
        locationHeader should endWith("/api-v3/property-definitions/datasets/dummyName/1")

        val response2 = sendGet[PropertyDefinition]("/property-definitions/datasets/dummyName/1")
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(propertyDefinition, actual)
        assert(actual == expected)
      }
      Seq(Some("default1"), None).foreach { suggestedValue =>
        s"a PropertyDefinition is created with most of default values (suggestedValue=$suggestedValue)" in {
          val propertyDefinition = minimalPdCreatePayload("smallPd", suggestedValue)
          val response = sendPostByAdmin[String, Validation](apiUrl, bodyOpt = Some(propertyDefinition))
          assertCreated(response)

          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/property-definitions/datasets/smallPd/1")

          val response2 = sendGet[PropertyDefinition]("/property-definitions/datasets/smallPd/1")
          assertOk(response2)

          val actual = response2.getBody
          val expected = toExpected(PropertyDefinition("smallPd", propertyType = StringPropertyType(suggestedValue)), actual)
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "an enabled PropertyDefinition with that name already exists" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
        propertyDefinitionFixture.add(propertyDefinition)

        val response = sendPostByAdmin[PropertyDefinition, Validation](apiUrl, bodyOpt = Some(propertyDefinition))
        assertBadRequest(response)

        val actual = response.getBody
        val expected = Validation().withError("name", "entity with name already exists: 'dummyName'")
        assert(actual == expected)
      }
      "an invalid PD payload is sent" in {
        val response = sendPostByAdmin[String, String](apiUrl, bodyOpt = Some(invalidPayload("somePd1")))
        assertBadRequest(response)

        response.getBody shouldBe "The suggested value invalidOptionC cannot be used: Value 'invalidOptionC' is not one of the allowed values (a, b)."
      }
    }

    "return 403" when {
      s"admin auth is not used for POST $apiUrl" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
        val response = sendPost[PropertyDefinition, String](apiUrl, bodyOpt = Some(propertyDefinition))
        response.getStatusCode shouldBe HttpStatus.FORBIDDEN
      }
    }
  }

  s"GET $apiUrl/{name}/used-in" should {
    "return 200" when {
      "there are used-in records" in {
        val propDefA1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propA", version = 1)
        val propDefA2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propA", version = 2, description = Some("An update"))
        val propDefB = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propB", version = 1) // moot
        propertyDefinitionFixture.add(propDefA1, propDefA2, propDefB)

        val datasetA1 = DatasetFactory.getDummyDataset(name = "datasetA", properties = Some(Map("propA" -> "something")))
        val datasetB1 = DatasetFactory.getDummyDataset(name = "datasetB", properties = Some(Map("propA" -> "something")), disabled = true)
        val datasetC1 = DatasetFactory.getDummyDataset(name = "datasetC", properties = Some(Map("propA" -> "something else")))
        datasetFixture.add(datasetA1, datasetB1, datasetC1)

        val response = sendGet[UsedIn](s"$apiUrl/propA/used-in")
        assertOk(response)

        // propDefB is moot.
        // datasetB is not reported, because it is disabled
        response.getBody shouldBe UsedIn(
          datasets = Some(Seq(MenasReference(None, "datasetA", 1), MenasReference(None, "datasetC", 1))),
          mappingTables = Some(Seq())
        )
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/used-in" should {
    "return 200" when {
      "there are used-in records for particular version" in {
        val propDefA1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propA", version = 1)
        val propDefA2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propA", version = 2, description = Some("An update"))
        propertyDefinitionFixture.add(propDefA1, propDefA2)

        val datasetA1 = DatasetFactory.getDummyDataset(name = "datasetA", properties = Some(Map("propA" -> "something")))
        val datasetB1 = DatasetFactory.getDummyDataset(name = "datasetB", properties = Some(Map("propA" -> "something")), disabled = true)
        val datasetC1 = DatasetFactory.getDummyDataset(name = "datasetC", properties = Some(Map("propA" -> "something else")))
        datasetFixture.add(datasetA1, datasetB1, datasetC1)

        val response = sendGet[UsedIn](s"$apiUrl/propA/1/used-in")
        assertOk(response)

        // same outcome as $apiUrl/{name}/used-in above -- because propDefs are not tied by version to datasets
        response.getBody shouldBe UsedIn(
          datasets = Some(Seq(MenasReference(None, "datasetA", 1), MenasReference(None, "datasetC", 1))),
          mappingTables = Some(Seq())
        )
      }
    }
  }

  s"DELETE $apiUrl/{name}" can {
    "return 200" when {
      "a PropertyDefinition with the given name exists" should {
        "disable the propertyDefinition with the given name" in {
          val propDefA1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propDefA", version = 1)
          val propDefA2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propDefA", version = 2)
          val propDefB = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propDefB", version = 1)
          propertyDefinitionFixture.add(propDefA1, propDefA2, propDefB)

          val response = sendDeleteByAdmin[DisabledPayload](s"$apiUrl/propDefA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[PropertyDefinition](s"$apiUrl/propDefA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[PropertyDefinition](s"$apiUrl/propDefA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true

          // unrelated propDef unaffected
          val responseB = sendGet[PropertyDefinition](s"$apiUrl/propDefB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe false
        }
      }

      "a PropertyDefinition with the given name exists and there have mixed (historical) disabled states " should {
        "disable all versions the propertyDefinition with the given name" in {
          val propDefA1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propDefA", version = 1, disabled = true)
          val propDefA2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propDefA", version = 2, disabled = false)
          propertyDefinitionFixture.add(propDefA1, propDefA2)

          val response = sendDeleteByAdmin[DisabledPayload](s"$apiUrl/propDefA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[PropertyDefinition](s"$apiUrl/propDefA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[PropertyDefinition](s"$apiUrl/propDefA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
      "the PropertyDefinition is only used in disabled Datasets" should {
        "disable the PropertyDefinition" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)

          val dataset = DatasetFactory.getDummyDataset(disabled = true, properties = Some(Map("propertyDefinition" -> "value xyz")))
          datasetFixture.add(dataset)

          val response = sendDeleteByAdmin[DisabledPayload](s"$apiUrl/propertyDefinition")

          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinition/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinition/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
    }

    "return 400" when {
      "the PropertyDefinition is used by an enabled Dataset" should {
        "return a list of the entities the PropertyDefinition is used in" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "keyA", version = 1)
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "keyA", version = 2, propertyType = EnumPropertyType("x", "y", "z"))
          val propertyDefinitionAsdf = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "keyASDF", version = 1) // moot support
          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2, propertyDefinitionAsdf)

          val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", properties = Some(Map("keyA" -> "x")))
          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 7, properties = Some(Map("keyA" -> "z")))
          val dataset3 = DatasetFactory.getDummyDataset(name = "dataset3", properties = Some(Map("keyASDF" -> "ASDF"))) // moot
          val disabledDs = DatasetFactory.getDummyDataset(name = "disabledDs", properties = Some(Map("keyA" -> "x")), disabled = true)
          datasetFixture.add(dataset1, dataset2, dataset3, disabledDs)

          val response = sendDeleteByAdmin[UsedIn](s"$apiUrl/keyA")

          assertBadRequest(response)
          response.getBody shouldBe UsedIn(Some(Seq(MenasReference(None, "dataset1", 1), MenasReference(None, "dataset2", 7))), Some(Seq()))
        }
      }
    }

    "return 404" when {
      "no PropertyDefinition with the given name exists" should {
        "disable nothing" in {
          val response = sendDeleteByAdmin[String](s"$apiUrl/aPropertyDefinition")
          assertNotFound(response)
        }
      }
    }

    "return 403" when {
      s"admin auth is not used for DELETE" in {
        val propertyDefinitionV1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionA", version = 1)
        propertyDefinitionFixture.add(propertyDefinitionV1)

        val response = sendDelete[Validation](s"$apiUrl/propertyDefinitionA")
        response.getStatusCode shouldBe HttpStatus.FORBIDDEN
      }
    }
  }
}
