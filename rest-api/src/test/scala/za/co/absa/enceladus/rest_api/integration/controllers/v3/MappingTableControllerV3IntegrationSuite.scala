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
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.dataFrameFilter._
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory, SchemaFactory}
import za.co.absa.enceladus.model.versionedModel.NamedVersion
import za.co.absa.enceladus.model.{DefaultValue, MappingTable, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.models.rest.{DisabledPayload, Paginated}
import za.co.absa.enceladus.rest_api.exceptions.EntityInUseException
import za.co.absa.enceladus.rest_api.integration.controllers.TestPaginatedMatchers.conformTo


@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class MappingTableControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val mappingTableFixture: MappingTableFixtureService = null

  @Autowired
  private val schemaFixture: SchemaFixtureService = null

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  private val apiUrl = "/mapping-tables"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(mappingTableFixture, schemaFixture, datasetFixture)

  // scalastyle:off magic.number - the test deliberately contains test actual data (no need for DRY here)
  s"GET $apiUrl" should {
    "return 200" when {
      "paginated mappingTable by default params (offset=0, limit=20)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val mappingTablesA = (1 to 15).map(i => MappingTableFactory.getDummyMappingTable(name = "mtA", version = i))
        val mappingTableA2disabled = MappingTableFactory.getDummyMappingTable(name = "mtA2", version = 1, disabled = true) // skipped in listing
        val mappingTablesB2M = ('B' to 'V').map(suffix => MappingTableFactory.getDummyMappingTable(name = s"mt$suffix"))
        mappingTableFixture.add(mappingTablesA: _*)
        mappingTableFixture.add(mappingTableA2disabled)
        mappingTableFixture.add(mappingTablesB2M: _*)

        val response = sendGet[TestPaginatedNamedVersion](s"$apiUrl")
        assertOk(response)
        response.getBody should conformTo(Paginated(offset = 0, limit = 20, truncated = true, page = Seq(
          NamedVersion("mtA", 15), NamedVersion("mtB", 1), NamedVersion("mtC", 1), NamedVersion("mtD", 1), NamedVersion("mtE", 1),
          NamedVersion("mtF", 1), NamedVersion("mtG", 1), NamedVersion("mtH", 1), NamedVersion("mtI", 1), NamedVersion("mtJ", 1),
          NamedVersion("mtK", 1), NamedVersion("mtL", 1), NamedVersion("mtM", 1), NamedVersion("mtN", 1), NamedVersion("mtO", 1),
          NamedVersion("mtP", 1), NamedVersion("mtQ", 1), NamedVersion("mtR", 1), NamedVersion("mtS", 1), NamedVersion("mtT", 1)
          // U, V are on the page 2
        )).asTestPaginated)
      }

      "paginated mappingTables with custom pagination (offset=10, limit=5)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val mappingTablesA2o = ('A' to 'O').map(suffix => MappingTableFactory.getDummyMappingTable(name = s"mt$suffix"))
        mappingTableFixture.add(mappingTablesA2o: _*)

        val response = sendGet[TestPaginatedNamedVersion](s"$apiUrl?offset=10&limit=5")
        assertOk(response)
        response.getBody should conformTo(Paginated(offset = 10, limit = 5, truncated = false, page = Seq(
          // A-E = page 1
          // F-J = page 2
          NamedVersion("mtK", 1), NamedVersion("mtL", 1), NamedVersion("mtM", 1), NamedVersion("mtN", 1), NamedVersion("mtO", 1)
          // no truncation
        )).asTestPaginated)
      }
      "paginated mappingTables as serialized string" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val mappingTablesA2o = ('A' to 'D').map(suffix => MappingTableFactory.getDummyMappingTable(name = s"mt$suffix"))
        mappingTableFixture.add(mappingTablesA2o: _*)

        val response = sendGet[String](s"$apiUrl?limit=3")
        assertOk(response)
        response.getBody shouldBe
          """{"page":[
            |{"name":"mtA","version":1,"disabled":false},
            |{"name":"mtB","version":1,"disabled":false},
            |{"name":"mtC","version":1,"disabled":false}
            |],
            |"offset":0,
            |"limit":3,
            |"truncated":true
            |}
            |""".stripMargin.replace("\n", "")
      }
    }
  }

  s"POST $apiUrl" should {
    "return 400" when {
      "referenced schema does not exits" in {
        val mtA = MappingTableFactory.getDummyMappingTable("mtA", schemaName = "mtSchemaA", schemaVersion = 1)

        val response = sendPost[MappingTable, Validation](apiUrl, bodyOpt = Some(mtA))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("schema" -> List("Schema mtSchemaA v1 not found!")))
      }
    }

    "return 201" when {
      "a MappingTables is created" in {
        val mtA = MappingTableFactory.getDummyMappingTable("mtA", schemaName = "mtSchema1", schemaVersion = 1)
        schemaFixture.add(SchemaFactory.getDummySchema("mtSchema1"))

        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // Schema referenced by MT must exist

        val response = sendPost[MappingTable, Validation](apiUrl, bodyOpt = Some(mtA))
        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/mapping-tables/mtA/1")

        val relativeLocation = stripBaseUrl(locationHeader) // because locationHeader contains domain, port, etc.
        val response2 = sendGet[MappingTable](stripBaseUrl(relativeLocation))
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(mtA, actual)

        assert(actual == expected)
      }

    }
  }

  // only MT-specific endpoints are covered further on:
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
      "the version is not the latest (only last version can be updated)" in {
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
      "the mapping table is disabled" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1, disabled = true)
          .copy(defaultMappingValue = List(DefaultValue("anOldDefault", "itsValue")))
        mappingTableFixture.add(mtAv1)
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // Schema referenced by MT must exist

        val response = sendPut[Array[DefaultValue], Validation](s"$apiUrl/mtA/1/defaults",
          bodyOpt = Some(Array.empty[DefaultValue]))
        assertBadRequest(response)

        response.getBody shouldBe Validation.empty.withError("disabled", "Entity mtA is disabled!")
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

          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // Schema referenced by MT must exist

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
      "the mapping table is disabled" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1, disabled = true)
          .copy(defaultMappingValue = List(DefaultValue("anOldDefault", "itsValue")))
        mappingTableFixture.add(mtAv1)
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // Schema referenced by MT must exist

        val response = sendPost[DefaultValue, Validation](s"$apiUrl/mtA/1/defaults", bodyOpt = Some(DefaultValue("colA", "defaultA")))
        assertBadRequest(response)
        response.getBody shouldBe Validation.empty.withError("disabled", "Entity mtA is disabled!")
      }
    }

    "201 Created with location" when {
      "defaults are replaced with a new version" in {
        val mtAv1 = MappingTableFactory.getDummyMappingTable("mtA", version = 1).copy(defaultMappingValue = List(DefaultValue("anOldDefault", "itsValue")))
        mappingTableFixture.add(mtAv1)

        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // Schema referenced by MT must exist

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

  private def mcr(mtName: String, mtVersion: Int, index: Int = 0) = MappingConformanceRule(index,
    controlCheckpoint = true,
    mappingTable = mtName,
    mappingTableVersion = mtVersion,
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

  s"GET $apiUrl/{name}/used-in" should {
    "return 200" when {
      "there are used-in records" in {
        val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 1)
        val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 2)
        mappingTableFixture.add(mappingTable1, mappingTable2)

        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA", conformance = List(mcr("mappingTable", 1)))
        val datasetB = DatasetFactory.getDummyDataset(name = "datasetB", conformance = List(mcr("mappingTable", 1)), disabled = true)
        val datasetC = DatasetFactory.getDummyDataset(name = "datasetC", conformance = List(mcr("mappingTable", 2)))
        datasetFixture.add(datasetA, datasetB, datasetC)

        val response = sendGet[String](s"$apiUrl/mappingTable/used-in")
        assertOk(response)

        // datasetB is disabled -> not reported
        // datasetC is reported, because this is a version-less check
        // String-typed this time to also check isEmpty/nonEmpty serialization presence
        response.getBody shouldBe
          """
            |{"datasets":[
            |{"collection":null,"name":"datasetA","version":1},{"collection":null,"name":"datasetC","version":1}
            |],
            |"mappingTables":null}
            |""".stripMargin.replaceAll("[\\r\\n]", "")
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/used-in" should {
    "return 200" when {
      "there are used-in records for particular version" in {
        val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 1)
        val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 2)
        mappingTableFixture.add(mappingTable1, mappingTable2)

        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA", conformance = List(mcr("mappingTable", 1)))
        val datasetB = DatasetFactory.getDummyDataset(name = "datasetB", conformance = List(mcr("mappingTable", 1)), disabled = true)
        val datasetC = DatasetFactory.getDummyDataset(name = "datasetC", conformance = List(mcr("mappingTable", 2)))
        datasetFixture.add(datasetA, datasetB, datasetC)

        val response = sendGet[UsedIn](s"$apiUrl/mappingTable/1/used-in")
        assertOk(response)

        // datasetB is disabled -> not reported
        // datasetC is not reported, because it depends on v2 of the MT
        response.getBody shouldBe UsedIn(
          datasets = Some(Seq(Reference(None, "datasetA", 1))),
          mappingTables = None
        )
      }

      "there are no used-in records for particular version" in {
        val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 1)
        mappingTableFixture.add(mappingTable1)

        val response = sendGet[UsedIn](s"$apiUrl/mappingTable/1/used-in")
        assertOk(response)

        response.getBody shouldBe UsedIn(
          datasets = None,
          mappingTables = None
        )
      }
    }
  }

  s"DELETE $apiUrl/{name}" can {
    "return 200" when {
      "a MappingTable with the given name exists" should {
        "disable the mappingTable with the given name" in {
          val mtA1 = MappingTableFactory.getDummyMappingTable(name = "mtA", version = 1)
          val mtA2 = MappingTableFactory.getDummyMappingTable(name = "mtA", version = 2)
          val mtB = MappingTableFactory.getDummyMappingTable(name = "mtB", version = 1)
          mappingTableFixture.add(mtA1, mtA2, mtB)

          val response = sendDelete[DisabledPayload](s"$apiUrl/mtA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[MappingTable](s"$apiUrl/mtA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[MappingTable](s"$apiUrl/mtA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true

          // unrelated mappingTable unaffected
          val responseB = sendGet[MappingTable](s"$apiUrl/mtB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe false
        }
      }

      "a MappingTable with the given name exists and there have mixed (historical) disabled states " should {
        "disable all versions the mappingTable with the given name" in {
          val mtA1 = MappingTableFactory.getDummyMappingTable(name = "mtA", version = 1, disabled = true)
          val mtA2 = MappingTableFactory.getDummyMappingTable(name = "mtA", version = 2, disabled = false)
          mappingTableFixture.add(mtA1, mtA2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/mtA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[MappingTable](s"$apiUrl/mtA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[MappingTable](s"$apiUrl/mtA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
      "the MappingTable is only used in disabled Datasets" should {
        "disable the MappingTable" in {
          val dataset = DatasetFactory.getDummyDataset(conformance = List(mcr("mappingTable", 1)), disabled = true)
          datasetFixture.add(dataset)
          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 1)
          val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 2)
          mappingTableFixture.add(mappingTable1, mappingTable2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/mappingTable")

          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[MappingTable](s"$apiUrl/mappingTable/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[MappingTable](s"$apiUrl/mappingTable/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
    }

    "return 400" when {
      "the MappingTable is used by an enabled Dataset" should {
        "return a list of the entities the MappingTable is used in" in {
          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 1)
          val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mappingTable", version = 2)
          mappingTableFixture.add(mappingTable1, mappingTable2)

          val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", conformance = List(mcr("mappingTable", 1)))
          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 7, conformance = List(mcr("mappingTable", 2)))
          val dataset3 = DatasetFactory.getDummyDataset(name = "dataset3", conformance = List(mcr("anotherMappingTable", 8))) // moot
          val disabledDs = DatasetFactory.getDummyDataset(name = "disabledDs", conformance = List(mcr("mappingTable", 2)), disabled = true)
          datasetFixture.add(dataset1, dataset2, dataset3, disabledDs)

          val response = sendDelete[EntityInUseException](s"$apiUrl/mappingTable")

          assertBadRequest(response)
          response.getBody shouldBe EntityInUseException("""Cannot disable entity "mappingTable", because it is used in the following entities""",
            UsedIn(Some(Seq(Reference(None, "dataset1", 1), Reference(None, "dataset2", 7))), None)
          )
        }
      }
    }

    "return 404" when {
      "no MappingTable with the given name exists" should {
        "disable nothing" in {
          val response = sendDelete[String](s"$apiUrl/aMappingTable")
          assertNotFound(response)
        }
      }
    }
  }

}
