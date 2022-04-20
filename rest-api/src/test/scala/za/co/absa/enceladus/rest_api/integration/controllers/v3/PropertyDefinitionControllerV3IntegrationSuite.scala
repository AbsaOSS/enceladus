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
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.Validation
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.propertyType.StringPropertyType
import za.co.absa.enceladus.model.test.factories.PropertyDefinitionFactory
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
  }
}
