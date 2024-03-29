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

package za.co.absa.enceladus.rest_api.integration.controllers

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.model.Validation
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.propertyType.StringPropertyType
import za.co.absa.enceladus.model.test.factories.PropertyDefinitionFactory

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class PropertyDefinitionApiIntegrationSuite extends BaseRestApiTestV2 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/properties/datasets"

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

  Seq(
    (s"$apiUrl/create", false), // VersionedModelController.create does not yield Location header
    (s"$apiUrl", true) // PropertyDefinitionController API "alias" does
  ).foreach { case (urlPattern, locationHeaderSupport) =>
    s"POST $urlPattern" can {
      "return 201" when {
        "a PropertyDefinition is created" should {
          "return the created PropertyDefinition" in {
            val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
            val response = sendPostByAdmin[PropertyDefinition, PropertyDefinition](urlPattern, bodyOpt = Some(propertyDefinition))
            assertCreated(response)
            if (locationHeaderSupport) {
              assert(response.getHeaders.getFirst("Location").contains("/api/properties/datasets/dummyName/1"))
            }

            val actual = response.getBody
            val expected = toExpected(propertyDefinition, actual)
            assert(actual == expected)
          }
        }
        "a PropertyDefinition is created with most of default values" should {
          Seq(Some("default1"), None).foreach { suggestedValue =>
            s"return the created PropertyDefinition (suggestedValue=$suggestedValue)" in {
              val propertyDefinition = minimalPdCreatePayload("smallPd", suggestedValue)
              val response = sendPostByAdmin[String, PropertyDefinition](urlPattern, bodyOpt = Some(propertyDefinition))
              assertCreated(response)

              val actual = response.getBody
              val expected = toExpected(PropertyDefinition("smallPd", propertyType = StringPropertyType(suggestedValue)), actual)
              assert(actual == expected)
            }
          }
        }
        "all prior versions of the PropertyDefinition are disabled" should {
          "return the recreated PropertyDefinition" in {
            val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
            propertyDefinitionFixture.add(propertyDefinition.copy(disabled = true))

            val response = sendPostByAdmin[PropertyDefinition, PropertyDefinition](urlPattern, bodyOpt = Some(propertyDefinition.setVersion(0)))
            assertCreated(response)

            val actual = response.getBody
            val expected = toExpected(propertyDefinition.setVersion(2).setParent(Some(PropertyDefinitionFactory.toParent(propertyDefinition))), actual)
            assert(actual == expected)
          }
        }
      }
      "return 400" when {
        "an enabled PropertyDefinition with that name already exists" in {
          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
          propertyDefinitionFixture.add(propertyDefinition)

          val response = sendPostByAdmin[PropertyDefinition, Validation](urlPattern, bodyOpt = Some(propertyDefinition))
          assertBadRequest(response)

          val actual = response.getBody
          val expected = Validation().withError("name", "entity with name already exists: 'dummyName'")
          assert(actual == expected)
        }
        "an invalid PD payload is sent" in {
          val response = sendPostByAdmin[String, String](urlPattern, bodyOpt = Some(invalidPayload("somePd1")))
          assertBadRequest(response)

          response.getBody shouldBe "The suggested value invalidOptionC cannot be used: Value 'invalidOptionC' is not one of the allowed values (a, b)."
        }
      }
    }
  }

  s"DELETE $apiUrl/disable/{name}" can {
    "return 200" when {
      "a PropertyDefinition with the given name exists" should {
        "disable only the propertyDefinition with the given name" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "otherPropertyDefinition", version = 1)
          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)

          val response = sendDeleteByAdmin[String](s"$apiUrl/disable/propertyDefinition")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "multiple versions of the PropertyDefinition with the given name exist" should {
        "disable all versions of the PropertyDefinition" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)

          val response = sendDeleteByAdmin[String](s"$apiUrl/disable/propertyDefinition")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
    }
  }

  s"DELETE $apiUrl/disable/{name}/{version}" can {
    "return 200" when {
      "a PropertyDefinition with the given name and version exists" should {
        "disable only the propertyDefinition with the given name and version" in {
          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "otherPropertyDefinition", version = 1)
          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)

          val response = sendDeleteByAdmin[String](s"$apiUrl/disable/propertyDefinition/1")
          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "multiple versions of the PropertyDefinition with the given name exist" should {
        Seq(
          ("disable only specific version of the propertyDefinition", s"$apiUrl/disable/propertyDefinition/1", 1),
          ("disable all versions by name of the propertyDefinition", s"$apiUrl/disable/propertyDefinition", 2)
        ).foreach { case (testCaseName, deleteUrl, expectedCount) =>
          testCaseName in {
            val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
            val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
            propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)

            val response = sendDeleteByAdmin[String](deleteUrl)
            assertOk(response)

            val actual = response.getBody
            val expected = s"""{"matchedCount":$expectedCount,"modifiedCount":$expectedCount,"upsertedId":null,"modifiedCountAvailable":true}"""
            assert(actual == expected)
          }
        }
      }

      "no PropertyDefinition with the given name exists" should {
        "disable nothing" in {
          val response = sendDeleteByAdmin[String](s"$apiUrl/disable/propertyDefinition/1")
          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":0,"modifiedCount":0,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
    }
  }

  s"GET $apiUrl/detail/{name}/latestVersion" should {
    "return 200" when {
      "a PropertyDefinition with the given name exists" in {
        val propDefV1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "property1", version = 1)
        val propDefV2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "property1",
          version = 2,
          parent = Some(PropertyDefinitionFactory.toParent(propDefV1)))
        propertyDefinitionFixture.add(propDefV1, propDefV2)

        val response = sendGet[String](s"$apiUrl/detail/property1/latestVersion")
        assertOk(response)
        assert("2" == response.getBody)
      }
    }

    "return 404" when {
      "a PropertyDefinition with the given name does not exist" in {
        val propDef = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "property1", version = 1)
        propertyDefinitionFixture.add(propDef)

        val response = sendGet[String](s"$apiUrl/detail/property2/latestVersion")
        assertNotFound(response)
      }
    }
  }


  s"GET $apiUrl/export/{name}/{version}" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/export/notFoundPropertyDefinition/2")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a correct PropertyDefinition version" should {
        "return the exported PD representation" in {
          val propDef = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
          propertyDefinitionFixture.add(propDef)
          val response = sendGet[String](s"$apiUrl/exportItem/propertyDefinition/2")

          assertOk(response)

          val body = response.getBody
          assert(body ==
            """{"metadata":{"exportVersion":1},"item":{
              |"name":"propertyDefinition",
              |"propertyType":{"_t":"StringPropertyType","suggestedValue":null},
              |"putIntoInfoFile":false,
              |"essentiality":{"_t":"Optional"}
              |}}""".stripMargin.replaceAll("[\\r\\n]", ""))
        }
      }
    }
  }


  // PD specific:
  Seq(
    s"$apiUrl/detail/{name}/{version}",
    s"$apiUrl/{name}/{version}" // PropertyDefinitionController API alias
  ).foreach {urlPattern =>
    s"GET $urlPattern" should {
      "return 404" when {
        "no propertyDefinition exists for the specified name" in {
          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
          propertyDefinitionFixture.add(propertyDefinition)

          val response = sendGet[String](urlPattern
            .replace("{name}", "otherPropertyDefinitionName")
            .replace("{version}", "1"))
          assertNotFound(response)
        }

        "no propertyDefinition exists for the specified version" in {
          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
          propertyDefinitionFixture.add(propertyDefinition)

          val response = sendGet[String](urlPattern
            .replace("{name}", "propertyDefinition1")
            .replace("{version}", "789"))
          assertNotFound(response)
        }
      }
      "return 200" when {
        "there is a PropertyDefinition with the specified name and version" should {
          "return the PropertyDefinition as a JSON" in {
            val pd22 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 22)
            val pd23 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 23,
              parent = Some(PropertyDefinitionFactory.toParent(pd22)))
            propertyDefinitionFixture.add(pd22, pd23)

            val response = sendGet[String](urlPattern
              .replace("{name}", "propertyDefinition1")
              .replace("{version}", "23"))
            assertOk(response)

            val body = response.getBody

            val expected =
              s"""{
                 |"name":"propertyDefinition1",
                 |"version":23,
                 |"description":null,
                 |"propertyType":{"_t":"StringPropertyType","suggestedValue":null},
                 |"putIntoInfoFile":false,
                 |"essentiality":{"_t":"Optional"},
                 |"disabled":false,
                 |"locked":null,
                 |"dateLocked":null,
                 |"userLocked":null,
                 |"dateCreated":"${pd23.dateCreated}",
                 |"userCreated":"dummyUser",
                 |"lastUpdated":"${pd23.lastUpdated}",
                 |"userUpdated":"dummyUser",
                 |"dateDisabled":null,
                 |"userDisabled":null,
                 |"parent":{"collection":"propertydef","name":"propertyDefinition1","version":22},
                 |"createdMessage":{
                 |"ref":{"collection":null,"name":"propertyDefinition1","version":23},
                 |"updatedBy":"dummyUser",
                 |"updated":"${pd23.createdMessage.updated}",
                 |"changes":[{"field":"","oldValue":null,"newValue":null,"message":"PropertyDefinition propertyDefinition1 created."}]
                 |}
                 |}""".stripMargin.replaceAll("[\\r\\n]", "")
            assert(body == expected)
          }
        }
      }
    }
  }
  Seq(
    s"$apiUrl/detail/{name}/latest",
    s"$apiUrl/{name}" // PropertyDefinitionController API alias
  ).foreach { urlPattern =>
    s"GET $urlPattern" should {
      "return 200" when {
        "there is a PropertyDefinition with the name and latest version (regardless of being disabled)" should {
          "return the PropertyDefinition as a JSON" in {
            val pd1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
            val pd2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 2)
            val pd3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 3, disabled = true)
            propertyDefinitionFixture.add(pd1, pd2, pd3)

            val response = sendGet[PropertyDefinition](urlPattern.replace("{name}", "propertyDefinition1"))
            assertOk(response)

            val bodyVersion = response.getBody.version
            assert(bodyVersion == 3)
          }
        }
      }
    }
  }

  s"GET $apiUrl" should {
    "return 200" when {
      "there is a list of PropertyDefinition in their latest non-disabled versions" should {
        "return the PropertyDefinition as a JSON" in {
          val pdA1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionA", version = 1)
          val pdA2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionA", version = 2)
          val pdA3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionA", version = 3, disabled = true)
          propertyDefinitionFixture.add(pdA1, pdA2, pdA3)

          val pdB1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionB", version = 1)
          val pdB2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionB", version = 2)
          val pdB3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinitionB", version = 3)
          propertyDefinitionFixture.add(pdB1, pdB2, pdB3)

          val response = sendGet[Array[PropertyDefinition]](s"$apiUrl") // Array to avoid erasure
          assertOk(response)

          val responseData = response.getBody.toSeq.map(pd => (pd.name, pd.version)).sortBy(_._1)
          val expectedData = Seq("propertyDefinitionA" -> 2, "propertyDefinitionB" -> 3) // disabled pdA-v3 not reported
          assert(responseData == expectedData)
        }
      }
    }
  }

}
