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
import za.co.absa.enceladus.model.{DefaultValue, Validation}
import za.co.absa.enceladus.model.test.factories.MappingTableFactory
import za.co.absa.enceladus.rest_api.integration.controllers.BaseRestApiTestV3
import za.co.absa.enceladus.rest_api.integration.fixtures._

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class MappingTableControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val mappingTableFixture: MappingTableFixtureService = null

  @Autowired
  private val schemaFixture: SchemaFixtureService = null

  private val apiUrl = "/mapping-tables"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(mappingTableFixture, schemaFixture)

  // todo create/update to show that schema presence is checked

  // only MT-specific endpoints are covered here
  s"GET $apiUrl/{name}/{version}/defaults" should {
    "return 404" when {
      "when the name/version does not exist" in {
        mappingTableFixture.add(MappingTableFactory.getDummyMappingTable("mtA"))

        assertNotFound(sendGet[String](s"$apiUrl/notFoundMt/456/defaults"))
        assertNotFound(sendGet[String](s"$apiUrl/mtA/456/defaults"))
        assertNotFound(sendGet[String](s"$apiUrl/notFoundMt/latest/defaults"))
      }
    }

    "return 200" when {
      "when there are no defaults" in {
        mappingTableFixture.add(MappingTableFactory.getDummyMappingTable("mtA"))

        val response = sendGet[Array[DefaultValue]](s"$apiUrl/mtA/1/defaults")

        assertOk(response)
        response.getBody shouldBe Seq()
      }

      "when there are some defaults rules (version \"latest\")" in {
        mappingTableFixture.add(
          MappingTableFactory.getDummyMappingTable("mtA"),
          MappingTableFactory.getDummyMappingTable("mtA", version = 2).copy(defaultMappingValue = List(
            DefaultValue("columnX", "defaultXvalue"),
            DefaultValue("columnY", "defaultYvalue")
          ))
        )


        val response = sendGet[Array[DefaultValue]](s"$apiUrl/mtA/latest/defaults")
        assertOk(response)
        response.getBody shouldBe Array(DefaultValue("columnX", "defaultXvalue"), DefaultValue("columnY", "defaultYvalue"))
      }
    }
  }

  s"PUT $apiUrl/{name}/{version}/defaults" should {
    "return 404" when {
      "when the name/version does not exist" in {
        mappingTableFixture.add(MappingTableFactory.getDummyMappingTable("mtA"))

        assertNotFound(sendPut[Array[DefaultValue], String](s"$apiUrl/notFoundMt/456/defaults", bodyOpt = Some(Array())))
        assertNotFound(sendPut[Array[DefaultValue], String](s"$apiUrl/mtA/456/defaults", bodyOpt = Some(Array())))
        assertNotFound(sendPut[Array[DefaultValue], String](s"$apiUrl/notFoundMt/latest/defaults", bodyOpt = Some(Array())))
      }
    }

    "return 400" when {
      "when version is not the latest (only last version can be updated)" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1)
        val mtAv2 = MappingTableFactory.getDummyMappingTable("mtA", version = 2)
        val mtAv3 = MappingTableFactory.getDummyMappingTable("mtA", version = 3)

        mappingTableFixture.add(mtAv1, mtAv2, mtAv3)

        val response = sendPut[Array[DefaultValue], Validation](s"$apiUrl/mtA/2/defaults", bodyOpt = Some(Array()))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("version" ->
          List("Version 2 of mtA is not the latest version, therefore cannot be edited")
        ))
      }
    }

    "201 Created with location" when {
      Seq(
        ("empty defaults", Array.empty[DefaultValue]),
        ("non-empty defaults", Array(DefaultValue("colA", "defaultA")))
      ).foreach { case (testCaseName, bothPayloadAndExpectedResult: Array[DefaultValue]) =>
        s"defaults are replaced with a new version ($testCaseName)" in {
          val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1).copy(defaultMappingValue = List(DefaultValue("anOldDefault", "itsValue")))
          mappingTableFixture.add(mtAv1)

          val response1 = sendPut[Array[DefaultValue], Validation](s"$apiUrl/mtA/1/defaults", bodyOpt = Some(bothPayloadAndExpectedResult))
          assertCreated(response1)
          response1.getBody shouldBe Validation.empty
          val headers1 = response1.getHeaders
          assert(headers1.getFirst("Location").endsWith("/api-v3/mapping-tables/mtA/2/defaults"))

          val response2 = sendGet[Array[DefaultValue]](s"$apiUrl/mtA/2/defaults")
          assertOk(response2)
          val responseBody = response2.getBody
          responseBody shouldBe bothPayloadAndExpectedResult // PUT is udd = 'anOldDefault' gets replaced, no trace of it
        }
      }
    }
  }

  s"POST $apiUrl/{name}/{version}/defaults" should {
    "return 404" when {
      "when the name/version does not exist" in {
        mappingTableFixture.add(MappingTableFactory.getDummyMappingTable("mtA"))

        val aDefaultValue = DefaultValue("colA", "defaultA")
        assertNotFound(sendPost[DefaultValue, String](s"$apiUrl/notFoundMt/456/defaults", bodyOpt = Some(aDefaultValue)))
        assertNotFound(sendPost[DefaultValue, String](s"$apiUrl/mtA/456/defaults", bodyOpt = Some(aDefaultValue)))
        assertNotFound(sendPost[DefaultValue, String](s"$apiUrl/notFoundMt/latest/defaults", bodyOpt = Some(aDefaultValue)))
      }
    }

    "return 400" when {
      "when version is not the latest (only last version can be updated)" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1)
        val mtAv2 = MappingTableFactory.getDummyMappingTable("mtA", version = 2)
        val mtAv3 = MappingTableFactory.getDummyMappingTable("mtA", version = 3)

        mappingTableFixture.add(mtAv1, mtAv2, mtAv3)

        val response = sendPost[DefaultValue, Validation](s"$apiUrl/mtA/2/defaults", bodyOpt = Some(DefaultValue("colA", "defaultA")))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("version" ->
          List("Version 2 of mtA is not the latest version, therefore cannot be edited")
        ))
      }
    }

    "201 Created with location" when {
      s"defaults are replaced with a new version" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1).copy(defaultMappingValue = List(DefaultValue("anOldDefault", "itsValue")))
        mappingTableFixture.add(mtAv1)

        val response1 = sendPost[DefaultValue, Validation](s"$apiUrl/mtA/1/defaults", bodyOpt = Some(DefaultValue("colA", "defaultA")))
        assertCreated(response1)
        response1.getBody shouldBe Validation.empty
        val headers1 = response1.getHeaders
        assert(headers1.getFirst("Location").endsWith("/api-v3/mapping-tables/mtA/2/defaults"))

        val response2 = sendGet[Array[DefaultValue]](s"$apiUrl/mtA/2/defaults")
        assertOk(response2)
        val responseBody = response2.getBody
        val expectedDefaults = Array(DefaultValue("anOldDefault", "itsValue"), DefaultValue("colA", "defaultA")) // POST = adding, 'anOldDefault' is kept
        responseBody shouldBe expectedDefaults
      }
    }
  }

}
