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
import za.co.absa.enceladus.model.Validation
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.PropertyDefinitionFactory
import za.co.absa.enceladus.model.versionedModel.NamedLatestVersion
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class PropertyDefinitionControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/property-definitions/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(propertyDefinitionFixture)


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

  s"GET $apiUrl/{name}" should {
    "return 200" when {
      "a propDef with the given name exists - so it gives versions" in {
        val pdV1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 1)
        val pdV2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA",
          version = 2, parent = Some(PropertyDefinitionFactory.toParent(pdV1)))
        propertyDefinitionFixture.add(pdV1, pdV2)

        val response = sendGet[NamedLatestVersion](s"$apiUrl/pdA")
        assertOk(response)
        assert(response.getBody == NamedLatestVersion("pdA", 2))
      }
    }

    "return 404" when {
      "a propDef with the given name does not exist" in {
        val pd = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 1)
        propertyDefinitionFixture.add(pd)

        val response = sendGet[String](s"$apiUrl/anotherDatasetName")
        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/{name}/{version}" should {
    "return 200" when {
      "a PropertyDefinition with the given name and version exists - gives specified version of entity" in {
        val pdV1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 1)
        val pdV2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 2, description = Some("second"))
        val pdV3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 3, description = Some("third"))
        propertyDefinitionFixture.add(pdV1, pdV2, pdV3)

        val response = sendGet[PropertyDefinition](s"$apiUrl/pdA/2")
        assertOk(response)

        val actual = response.getBody
        val expected = toExpected(pdV2, actual)

        assert(actual == expected)
      }
    }

    "return 404" when {
      "a PropertyDefinition with the given name/version does not exist" in {
        val pd = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "pdA", version = 1)
        propertyDefinitionFixture.add(pd)

        val response = sendGet[String](s"$apiUrl/anotherPropertyDefinitionName/1")
        assertNotFound(response)

        val response2 = sendGet[String](s"$apiUrl/pdA/7")
        assertNotFound(response2)
      }
    }
  }

  s"PUT $apiUrl/{name}/{version}" can {
    "return 200" when {
      "a PropertyDefinition with the given name and version is the latest that exists" should {
        "update the propertyDefinition" in {
          val propertyDefinitionA1 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA")
          val propertyDefinitionA2 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA",
            description = Some("second version"), version = 2)
          propertyDefinitionFixture.add(propertyDefinitionA1, propertyDefinitionA2)

          val propertyDefinitionA3 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA",
            description = Some("updated"),
            propertyType = EnumPropertyType("a", "b"),
            version = 2 // update references the last version
          )

          val response = sendPutByAdmin[PropertyDefinition, Validation](s"$apiUrl/propertyDefinitionA/2", bodyOpt = Some(propertyDefinitionA3))
          assertCreated(response)
          response.getBody shouldBe Validation.empty
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/property-definitions/datasets/propertyDefinitionA/3")

          val response2 = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinitionA/3")
          assertOk(response2)

          val actual = response2.getBody
          val expected = toExpected(propertyDefinitionA3.copy(version = 3, parent = Some(PropertyDefinitionFactory.toParent(propertyDefinitionA2))), actual)
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "a PropertyDefinition with the given name and version" should {
        "fail when version/name in the URL and payload is mismatched" in {
          val propertyDefinitionA1 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA", description = Some("init version"))
          propertyDefinitionFixture.add(propertyDefinitionA1)

          val response = sendPutByAdmin[PropertyDefinition, String](s"$apiUrl/propertyDefinitionA/7",
            bodyOpt = Some(PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA", version = 5)))
          response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response.getBody should include("version mismatch: 7 != 5")

          val response2 = sendPutByAdmin[PropertyDefinition, String](s"$apiUrl/propertyDefinitionABC/4",
            bodyOpt = Some(PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionXYZ", version = 4)))
          response2.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response2.getBody should include("name mismatch: 'propertyDefinitionABC' != 'propertyDefinitionXYZ'")
        }
      }
    }

    "return 403" when {
      s"admin auth is not used" in {
        val propertyDefinitionA1 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA")
        propertyDefinitionFixture.add(propertyDefinitionA1)

        val propertyDefinitionA2 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA",
          description = Some("updated"),
          propertyType = EnumPropertyType("a", "b"),
          version = 1 // update references the last version
        )

        val response = sendPut[PropertyDefinition, String](s"$apiUrl/propertyDefinitionA/1", bodyOpt = Some(propertyDefinitionA2))
        response.getStatusCode shouldBe HttpStatus.FORBIDDEN
      }
    }

    "return 404" when {
      "a propDef with the given name does not exist" in {
        val propertyDefinitionA2 = PropertyDefinitionFactory.getDummyPropertyDefinition("propertyDefinitionA")

        val response = sendPutByAdmin[PropertyDefinition, String](s"$apiUrl/propertyDefinitionA/1", bodyOpt = Some(propertyDefinitionA2))
        assertNotFound(response)
      }
    }

  }

  s"POST $apiUrl/{name}/import" should {
    val importablePd =
      """{"todo":{"exportVersion":1},"item":{
        |"name":"propertyDefinitionXYZ",
        |"description":"Hi, I am the import",
        |"propertyType":{"_t":"StringPropertyType"},
        |"putIntoInfoFile":false,
        |"essentiality":{"_t":"Optional"}
        |}}""".stripMargin.replaceAll("[\\r\\n]", "")

    "return 400" when {
      "a PropertyDefinition with the given name" should {
        "fail when name in the URL and payload is mismatched" in {
          val response = sendPostByAdmin[String, String](s"$apiUrl/propertyDefinitionABC/import",
            bodyOpt = Some(importablePd))
          response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response.getBody should include("name mismatch: 'propertyDefinitionABC' != 'propertyDefinitionXYZ'")
        }
      }
    }

    "return 403" when {
      s"admin auth is not used" in {
        val response = sendPost[String, Validation](s"$apiUrl/propertyDefinitionXYZ/import", bodyOpt = Some(importablePd))
        response.getStatusCode shouldBe HttpStatus.FORBIDDEN
      }
    }

    "return 201" when {
      "there is a existing PropertyDefinition" should {
        "a +1 version of propertyDefinition is added" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionXYZ", description = Some("init version"))
          propertyDefinitionFixture.add(propertyDefinition1)

          val response = sendPostByAdmin[String, Validation](s"$apiUrl/propertyDefinitionXYZ/import", bodyOpt = Some(importablePd))
          assertCreated(response)
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/property-definitions/datasets/propertyDefinitionXYZ/2")
          response.getBody shouldBe Validation.empty

          val response2 = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinitionXYZ/2")
          assertOk(response2)

          val actual = response2.getBody
          val expectedPdBase = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionXYZ", version = 2,
            description = Some("Hi, I am the import"),
            parent = Some(PropertyDefinitionFactory.toParent(propertyDefinition1))
          )
          val expected = toExpected(expectedPdBase, actual)

          assert(actual == expected)
        }
      }

      "there is no such PropertyDefinition, yet" should {
        "a the version of propertyDefinition created" in {
          val response = sendPostByAdmin[String, String](s"$apiUrl/propertyDefinitionXYZ/import", bodyOpt = Some(importablePd))
          assertCreated(response)
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/property-definitions/datasets/propertyDefinitionXYZ/1") // this is the first version

          val response2 = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinitionXYZ/1")
          assertOk(response2)

          val actual = response2.getBody
          val expectedDsBase = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionXYZ",
            description = Some("Hi, I am the import"))
          val expected = toExpected(expectedDsBase, actual)

          assert(actual == expected)
        }
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/export" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundPropertyDefinition/2/export")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a correct PropertyDefinition version" should {
        "return the exported PropertyDefinition representation" in {
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2, description = Some("v2 here"))
          val propertyDefinition3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 3, description = Some("showing non-latest export"))
          propertyDefinitionFixture.add(propertyDefinition2, propertyDefinition3)
          val response = sendGet[String](s"$apiUrl/propertyDefinition/2/export")

          assertOk(response)

          val body = response.getBody
          assert(body ==
            """{"metadata":{"exportVersion":1},"item":{
              |"name":"propertyDefinition",
              |"description":"v2 here",
              |"propertyType":{"_t":"StringPropertyType","suggestedValue":null},
              |"putIntoInfoFile":false,
              |"essentiality":{"_t":"Optional"}
              |}}""".stripMargin.replaceAll("[\\r\\n]", ""))
        }
      }
    }
  }

  // todo delete
}
