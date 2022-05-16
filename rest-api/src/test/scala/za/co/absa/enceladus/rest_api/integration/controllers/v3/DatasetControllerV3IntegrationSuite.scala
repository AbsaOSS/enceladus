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
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, LiteralConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.dataFrameFilter._
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.propertyType.EnumPropertyType
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory, PropertyDefinitionFactory, SchemaFactory}
import za.co.absa.enceladus.model.versionedModel.NamedVersion
import za.co.absa.enceladus.model.{Dataset, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.exceptions.EntityDisabledException
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.models.rest.DisabledPayload

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val schemaFixture: SchemaFixtureService = null

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  @Autowired
  private val mappingTableFixture: MappingTableFixtureService = null

  private val apiUrl = "/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(datasetFixture, propertyDefinitionFixture, schemaFixture, mappingTableFixture)


  s"POST $apiUrl" should {
    "return 201" when {
      "a Dataset is created" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset = DatasetFactory.getDummyDataset("dummyDs",
          properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")))
        propertyDefinitionFixture.add(
          PropertyDefinitionFactory.getDummyPropertyDefinition("keyA"),
          PropertyDefinitionFactory.getDummyPropertyDefinition("keyB"),
          PropertyDefinitionFactory.getDummyPropertyDefinition("keyC"),
          PropertyDefinitionFactory.getDummyPropertyDefinition("keyD", essentiality = Essentiality.Recommended)
        )

        val response = sendPost[Dataset, Validation](apiUrl, bodyOpt = Some(dataset))
        assertCreated(response)
        response.getBody shouldBe Validation.empty.withWarning("keyD", "Property 'keyD' is recommended to be present, but was not found!")
        val locationHeader = response.getHeaders.getFirst("location")
        locationHeader should endWith("/api-v3/datasets/dummyDs/1")

        val relativeLocation = stripBaseUrl(locationHeader) // because locationHeader contains domain, port, etc.
        val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(dataset, actual).copy(properties = Some(Map("keyA" -> "valA", "keyB" -> "valB"))) // keyC stripped

        assert(actual == expected)
      }
    }

    "return 400" when {
      "dataset schema does not exits" in {
        val dataset = DatasetFactory.getDummyDataset("dummyDs")
        // there are schemas defined

        val response = sendPost[Dataset, Validation](apiUrl, bodyOpt = Some(dataset))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("schema" -> List("Schema dummySchema v1 not found!")))
      }

      "datasets properties are not backed by propDefs (undefined properties)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset = DatasetFactory.getDummyDataset("dummyDs", properties = Some(Map("undefinedProperty1" -> "value1")))
        // propdefs are empty

        val response = sendPost[Dataset, Validation](apiUrl, bodyOpt = Some(dataset))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("undefinedProperty1" -> List("There is no property definition for key 'undefinedProperty1'.")))
      }
      "disabled entity with the name already exists" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset1 = DatasetFactory.getDummyDataset("dummyDs", disabled = true)
        datasetFixture.add(dataset1)

        val dataset2 = DatasetFactory.getDummyDataset("dummyDs", description = Some("a new version attempt"))
        val response = sendPost[Dataset, EntityDisabledException](apiUrl, bodyOpt = Some(dataset2))

        assertBadRequest(response)
        response.getBody.getMessage should include("Entity dummyDs is disabled. Enable it first")
      }
    }

  }

  s"GET $apiUrl/{name}" should {
    "return 200" when {
      "a Dataset with the given name exists - so it gives versions" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA",
          version = 2,
          parent = Some(DatasetFactory.toParent(datasetV1)))
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[NamedVersion](s"$apiUrl/datasetA")
        assertOk(response)
        assert(response.getBody == NamedVersion("datasetA", 2, disabled = false))
      }

      "a Dataset with the given name exists - all disabled" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1, disabled = true)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, disabled = true)
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[NamedVersion](s"$apiUrl/datasetA")
        assertOk(response)
        assert(response.getBody == NamedVersion("datasetA", 2, disabled = true))
      }

      "a Dataset with with mixed disabled states" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, disabled = true)
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[NamedVersion](s"$apiUrl/datasetA")
        assertOk(response)
        assert(response.getBody == NamedVersion("datasetA", 2, disabled = true)) // mixed state -> disabled
      }
    }

    "return 404" when {
      "a Dataset with the given name does not exist" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/anotherDatasetName")
        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/{name}/latest" should {
    "return 200" when {
      "a Dataset with the given name exists - gives latest version entity" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA",
          version = 2, parent = Some(DatasetFactory.toParent(datasetV1)))
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[Dataset](s"$apiUrl/datasetA/latest")
        assertOk(response)

        val actual = response.getBody
        val expected = toExpected(datasetV2, actual)

        assert(actual == expected)
      }
    }

    "return 404" when {
      "a Dataset with the given name does not exist" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/anotherDatasetName/latest")
        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/{name}/{version}" should {
    "return 200" when {
      "a Dataset with the given name and version exists - gives specified version of entity" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, description = Some("second"))
        val datasetV3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3, description = Some("third"))
        datasetFixture.add(datasetV1, datasetV2, datasetV3)

        val response = sendGet[Dataset](s"$apiUrl/datasetA/2")
        assertOk(response)

        val actual = response.getBody
        val expected = toExpected(datasetV2, actual)

        assert(actual == expected)
      }
    }

    "return 404" when {
      "a Dataset with the given name/version does not exist" in {
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/anotherDatasetName/1")
        assertNotFound(response)

        val response2 = sendGet[String](s"$apiUrl/datasetA/7")
        assertNotFound(response2)
      }
    }
  }

  private val exampleMappingCr = MappingConformanceRule(0,
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

  s"PUT $apiUrl/{name}/{version}" can {
    "return 200" when {
      "a Dataset with the given name and version is the latest that exists" should {
        "update the dataset (with empty properties stripped)" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))

          val datasetA1 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("init version"), properties = Some(Map("keyA" -> "valA")))
          val datasetA2 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("second version"), properties = Some(Map("keyA" -> "valA")), version = 2)
          datasetFixture.add(datasetA1, datasetA2)

          Seq("keyA", "keyB", "keyC").foreach { propName =>
            propertyDefinitionFixture.add(
              PropertyDefinitionFactory.getDummyPropertyDefinition(propName, essentiality = Essentiality.Optional)
            )
          }
          // this will cause missing property 'keyD' to issue a warning if not present
          propertyDefinitionFixture.add(
            PropertyDefinitionFactory.getDummyPropertyDefinition("keyD", essentiality = Essentiality.Recommended)
          )

          mappingTableFixture.add(MappingTableFactory.getDummyMappingTable("CurrencyMappingTable", version = 9)) //scalastyle:ignore magic.number

          val datasetA3 = DatasetFactory.getDummyDataset("datasetA",
            description = Some("updated"),
            properties = Some(Map("keyA" -> "valA", "keyB" -> "valB", "keyC" -> "")),
            conformance = List(exampleMappingCr),
            version = 2 // update references the last version
          )

          val response = sendPut[Dataset, Validation](s"$apiUrl/datasetA/2", bodyOpt = Some(datasetA3))
          assertCreated(response)
          response.getBody shouldBe Validation.empty.withWarning("keyD", "Property 'keyD' is recommended to be present, but was not found!")
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/datasets/datasetA/3")

          val relativeLocation = stripBaseUrl(locationHeader) // because locationHeader contains domain, port, etc.
          val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
          assertOk(response2)

          val actual = response2.getBody
          val expected = toExpected(datasetA3.copy(version = 3, parent = Some(DatasetFactory.toParent(datasetA2)), properties = Some(Map("keyA" -> "valA", "keyB" -> "valB"))), actual) // blank property stripped

          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "when properties are not backed by propDefs (undefined properties) and schema does not exist" in {
        val datasetA1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(datasetA1)
        // propdefs are empty, schemas not defined

        val datasetA2 = DatasetFactory.getDummyDataset("datasetA",
          description = Some("second version"), properties = Some(Map("keyA" -> "valA"))) // version in payload is irrelevant

        val response = sendPut[Dataset, Validation](s"$apiUrl/datasetA/1", bodyOpt = Some(datasetA2))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe
          Validation(Map(
            "schema" -> List("Schema dummySchema v1 not found!"),
            "keyA" -> List("There is no property definition for key 'keyA'.")
          ))
      }

      "a Dataset with the given name and version" should {
        "fail when version/name in the URL and payload is mismatched" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
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
      "entity is disabled" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset1 = DatasetFactory.getDummyDataset("dummyDs", disabled = true)
        datasetFixture.add(dataset1)

        val dataset2 = DatasetFactory.getDummyDataset("dummyDs", description = Some("ds update"))
        val response = sendPut[Dataset, EntityDisabledException](s"$apiUrl/dummyDs/1", bodyOpt = Some(dataset2))

        assertBadRequest(response)
        response.getBody.getMessage should include("Entity dummyDs is disabled. Enable it first")
      }
    }
  }

  s"GET $apiUrl/{name}/audit-trail" should {
    "return 404" when {
      "when the name does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/audit-trail")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a correct Dataset" should {
        "return an audit trail for the dataset" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
          val dataset1 = DatasetFactory.getDummyDataset(name = "datasetA")
          val dataset2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2,
            conformance = List(LiteralConformanceRule(0, "outputCol1", controlCheckpoint = false, "litValue1")),
            parent = Some(DatasetFactory.toParent(dataset1))
          )

          datasetFixture.add(dataset1, dataset2)
          val response = sendGet[String](s"$apiUrl/datasetA/audit-trail")

          assertOk(response)

          val body = response.getBody
          assert(body ==
            """{"entries":[{
              |"menasRef":{"collection":null,"name":"datasetA","version":2},
              |"updatedBy":"dummyUser","updated":"2017-12-04T16:19:17Z",
              |"changes":[{"field":"conformance","oldValue":null,"newValue":"LiteralConformanceRule(0,outputCol1,false,litValue1)","message":"Conformance rule added."}]
              |},{
              |"menasRef":{"collection":null,"name":"datasetA","version":1},
              |"updatedBy":"dummyUser","updated":"2017-12-04T16:19:17Z",
              |"changes":[{"field":"","oldValue":null,"newValue":null,"message":"Dataset datasetA created."}]
              |}]}""".stripMargin.replaceAll("[\\r\\n]", ""))
        }
      }
    }
  }

  s"POST $apiUrl/{name}/import" should {
    val importableDs =
      """{"metadata":{"exportVersion":1},"item":{
        |"name":"datasetXYZ",
        |"description":"Hi, I am the import",
        |"hdfsPath":"/dummy/path",
        |"hdfsPublishPath":"/dummy/publish/path",
        |"schemaName":"dummySchema",
        |"schemaVersion":1,
        |"conformance":[{"_t":"LiteralConformanceRule","order":0,"outputColumn":"outputCol1","controlCheckpoint":false,"value":"litValue1"}],
        |"properties":{"key2":"val2","key1":"val1"}
        |}}""".stripMargin.replaceAll("[\\r\\n]", "")

    "return 400" when {
      "a Dataset with the given name" should {
        "fail when name in the URL and payload is mismatched" in {
          val response = sendPost[String, String](s"$apiUrl/datasetABC/import",
            bodyOpt = Some(importableDs))
          response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
          response.getBody should include("name mismatch: 'datasetABC' != 'datasetXYZ'")
        }
      }
      "imported Dataset fails validation" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        propertyDefinitionFixture.add(PropertyDefinitionFactory.getDummyPropertyDefinition("key1")) // key2 propdef is missing

        val response = sendPost[String, Validation](s"$apiUrl/datasetXYZ/import", bodyOpt = Some(importableDs))

        response.getStatusCode shouldBe HttpStatus.BAD_REQUEST
        response.getBody shouldBe Validation.empty.withError("key2", "There is no property definition for key 'key2'.")
      }
    }

    "return 201" when {
      "there is a existing Dataset" should {
        "a +1 version of dataset is added" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // import feature checks schema presence
          val dataset1 = DatasetFactory.getDummyDataset(name = "datasetXYZ", description = Some("init version"))
          datasetFixture.add(dataset1)

          propertyDefinitionFixture.add(
            PropertyDefinitionFactory.getDummyPropertyDefinition("key1"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("key2"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("key3", essentiality = Essentiality.Recommended)
          )

          val response = sendPost[String, Validation](s"$apiUrl/datasetXYZ/import", bodyOpt = Some(importableDs))
          assertCreated(response)
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/datasets/datasetXYZ/2")
          response.getBody shouldBe Validation.empty.withWarning("key3", "Property 'key3' is recommended to be present, but was not found!")

          val relativeLocation = stripBaseUrl(locationHeader) // because locationHeader contains domain, port, etc.
          val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
          assertOk(response2)

          val actual = response2.getBody
          val expectedDsBase = DatasetFactory.getDummyDataset(name = "datasetXYZ", version = 2, description = Some("Hi, I am the import"),
            properties = Some(Map("key1" -> "val1", "key2" -> "val2")),
            conformance = List(LiteralConformanceRule(0, "outputCol1", controlCheckpoint = false, "litValue1")),
            parent = Some(DatasetFactory.toParent(dataset1))
          )
          val expected = toExpected(expectedDsBase, actual)

          assert(actual == expected)
        }
      }

      "there is no such Dataset, yet" should {
        "a the version of dataset created" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema")) // import feature checks schema presence
          propertyDefinitionFixture.add(
            PropertyDefinitionFactory.getDummyPropertyDefinition("key1"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("key2")
          )

          val response = sendPost[String, String](s"$apiUrl/datasetXYZ/import", bodyOpt = Some(importableDs))
          assertCreated(response)
          val locationHeader = response.getHeaders.getFirst("location")
          locationHeader should endWith("/api-v3/datasets/datasetXYZ/1") // this is the first version

          val relativeLocation = stripBaseUrl(locationHeader) // because locationHeader contains domain, port, etc.
          val response2 = sendGet[Dataset](stripBaseUrl(relativeLocation))
          assertOk(response2)

          val actual = response2.getBody
          val expectedDsBase = DatasetFactory.getDummyDataset(name = "datasetXYZ", description = Some("Hi, I am the import"),
            properties = Some(Map("key1" -> "val1", "key2" -> "val2")),
            conformance = List(LiteralConformanceRule(0, "outputCol1", controlCheckpoint = false, "litValue1"))
          )
          val expected = toExpected(expectedDsBase, actual)

          assert(actual == expected)
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
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
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

  s"GET $apiUrl/{name}/used-in" should {
    "return 404" when {
      "when the dataset of name does not exist" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA")
        datasetFixture.add(datasetA)

        val response = sendGet[String](s"$apiUrl/notFoundDataset/used-in")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "any exiting latest dataset" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA")
        datasetFixture.add(datasetA)
        val response = sendGet[UsedIn](s"$apiUrl/datasetA/used-in")
        assertOk(response)

        response.getBody shouldBe UsedIn(None, None)
      }
    }

    "return 200" when {
      "for existing name for dataset" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2)
        datasetFixture.add(dataset2)
        val response = sendGet[UsedIn](s"$apiUrl/dataset/used-in")

        assertOk(response)
        response.getBody shouldBe UsedIn(None, None)
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/properties" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/456/properties")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a specific Dataset version" should {
        Seq(
          ("empty1", Some(Map.empty[String, String])),
          ("empty2", None),
          ("non-empty", Some(Map("key1" -> "val1", "key2" -> "val2")))
        ).foreach { case (propertiesCaseName, propertiesData) =>
          s"return dataset properties ($propertiesCaseName)" in {
            schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
            val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
            val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, properties = propertiesData)
            val datasetV3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3, properties = Some(Map("other" -> "prop")))
            datasetFixture.add(datasetV1, datasetV2, datasetV3)

            val response = sendGet[Map[String, String]](s"$apiUrl/datasetA/2/properties")
            assertOk(response)

            val expectedProperties = propertiesData.getOrElse(Map.empty[String, String])
            val body = response.getBody
            assert(body == expectedProperties)
          }
        }
      }
    }

    "return 200" when {
      "there is a latest Dataset version" should {
        s"return dataset properties" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
          val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
          val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2, properties = Some(Map("key1" -> "val1", "key2" -> "val2")))
          datasetFixture.add(datasetV1, datasetV2)

          val response = sendGet[Map[String, String]](s"$apiUrl/datasetA/latest/properties")
          assertOk(response)

          val body = response.getBody
          assert(body == Map("key1" -> "val1", "key2" -> "val2"))
        }
      }
    }
  }

  s"PUT $apiUrl/{name}/{version}/properties" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendPut[Map[String, String], String](s"$apiUrl/notFoundDataset/456/properties", bodyOpt = Some(Map.empty))
        assertNotFound(response)
      }
    }

    "return 400" when {
      "when version is not the latest (only last version can be updated)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2)
        val datasetV3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
        datasetFixture.add(datasetV1, datasetV2, datasetV3)

        val response = sendPut[Map[String, String], Validation](s"$apiUrl/datasetA/2/properties", bodyOpt = Some(Map.empty))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("version" ->
          List("Version 2 of datasetA is not the latest version, therefore cannot be edited")
        ))
      }

      "when properties are not backed by propDefs (undefined properties)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(datasetV1)
        // propdefs are empty

        val response = sendPut[Map[String, String], Validation](s"$apiUrl/datasetA/1/properties",
          bodyOpt = Some(Map("undefinedProperty1" -> "someValue")))

        assertBadRequest(response)
        val responseBody = response.getBody
        responseBody shouldBe Validation(Map("undefinedProperty1" -> List("There is no property definition for key 'undefinedProperty1'.")))
      }

      "when properties are not valid (based on propDefs)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(datasetV1)

        propertyDefinitionFixture.add(
          PropertyDefinitionFactory.getDummyPropertyDefinition("mandatoryA", essentiality = Essentiality.Mandatory),
          PropertyDefinitionFactory.getDummyPropertyDefinition("AorB", propertyType = EnumPropertyType("a", "b"))
        )

        val response1 = sendPut[Map[String, String], Validation](s"$apiUrl/datasetA/1/properties",
          bodyOpt = Some(Map("AorB" -> "a"))) // this is ok, but mandatoryA is missing

        assertBadRequest(response1)
        response1.getBody shouldBe Validation(Map("mandatoryA" -> List("Dataset property 'mandatoryA' is mandatory, but does not exist!")))

        val response2 = sendPut[Map[String, String], Validation](s"$apiUrl/datasetA/1/properties",
          bodyOpt = Some(Map("mandatoryA" -> "valueA", "AorB" -> "c"))) // mandatoryA is ok, but AorB has invalid value

        assertBadRequest(response2)
        response2.getBody shouldBe Validation(Map("AorB" -> List("Value 'c' is not one of the allowed values (a, b).")))
      }
    }

    "201 Created with location" when {
      Seq(
        ("non-empty properties map", """{"keyA":"valA","keyB":"valB","keyC":""}""", Some(Map("keyA" -> "valA", "keyB" -> "valB"))), // empty string property would get removed (defined "" => undefined)
        ("empty properties map", "{}", Some(Map.empty))
      ).foreach { case (testCaseName, payload, expectedPropertiesSet) =>
        s"properties are replaced with a new version ($testCaseName)" in {
          schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
          val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
          datasetFixture.add(datasetV1)

          propertyDefinitionFixture.add(
            PropertyDefinitionFactory.getDummyPropertyDefinition("keyA"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("keyB"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("keyC"),
            PropertyDefinitionFactory.getDummyPropertyDefinition("keyD", essentiality = Essentiality.Recommended)
          )

          val response1 = sendPut[String, Validation](s"$apiUrl/datasetA/1/properties", bodyOpt = Some(payload))
          assertCreated(response1)
          response1.getBody shouldBe Validation.empty.withWarning("keyD", "Property 'keyD' is recommended to be present, but was not found!")
          val headers1 = response1.getHeaders
          assert(headers1.getFirst("Location").endsWith("/api-v3/datasets/datasetA/2/properties"))

          val response2 = sendGet[Map[String, String]](s"$apiUrl/datasetA/2/properties")
          assertOk(response2)
          val responseBody = response2.getBody
          responseBody shouldBe expectedPropertiesSet.getOrElse(Map.empty)
        }
      }
    }
  }

  // similar to put-properties validation
  s"GET $apiUrl/{name}/{version}/validation" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/456/validation")
        assertNotFound(response)
      }
    }

    // todo name validation - common for versioned entities

    "return 200" when {
      "when properties are not backed by propDefs (undefined properties) and schema is missing" in {
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", properties = Some(Map("undefinedProperty1" -> "someValue")))
        datasetFixture.add(datasetV1)
        // propdefs are empty, schemas not defined

        val response = sendGet[Validation](s"$apiUrl/datasetA/1/validation")

        assertOk(response)
        response.getBody shouldBe
          Validation(Map(
            "undefinedProperty1" -> List("There is no property definition for key 'undefinedProperty1'."),
            "schema" -> List("Schema dummySchema v1 not found!")
          ))
      }

      "when properties are not valid (based on propDefs) - mandatoriness check" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", properties = None) // prop 'mandatoryA' not present
        datasetFixture.add(datasetV1)

        propertyDefinitionFixture.add(
          PropertyDefinitionFactory.getDummyPropertyDefinition("mandatoryA", essentiality = Essentiality.Mandatory)
        )

        val response = sendGet[Validation](s"$apiUrl/datasetA/1/validation")
        assertOk(response)
        response.getBody shouldBe Validation(Map("mandatoryA" -> List("Dataset property 'mandatoryA' is mandatory, but does not exist!")))
      }

      "when properties are not valid (based on propDefs) - property conformance" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", properties = Some(Map("AorB" -> "c")))
        datasetFixture.add(datasetV1)

        propertyDefinitionFixture.add(
          PropertyDefinitionFactory.getDummyPropertyDefinition("AorB", propertyType = EnumPropertyType("a", "b"))
        )

        val response = sendGet[Validation](s"$apiUrl/datasetA/1/validation")
        assertOk(response)
        response.getBody shouldBe Validation(Map("AorB" -> List("Value 'c' is not one of the allowed values (a, b).")))
      }
    }
  }

  private val exampleMcrRule0 = MappingConformanceRule(0,
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

  private val exampleLitRule1 = LiteralConformanceRule(order = 1, controlCheckpoint = true, outputColumn = "something", value = "1.01")
  private val dsWithRules1 = DatasetFactory.getDummyDataset(name = "datasetA", conformance = List(
    exampleMcrRule0, exampleLitRule1
  ))

  s"GET $apiUrl/{name}/{version}/rules" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/456/rules")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "when there are no conformance rules" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA")
        datasetFixture.add(datasetV1)

        val response = sendGet[Array[ConformanceRule]](s"$apiUrl/datasetA/1/rules")

        assertOk(response)
        response.getBody shouldBe Seq()
      }

      "when there are some conformance rules" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        datasetFixture.add(dsWithRules1)

        val response = sendGet[Array[ConformanceRule]](s"$apiUrl/datasetA/1/rules")
        assertOk(response)
        response.getBody shouldBe dsWithRules1.conformance.toArray
      }
    }
  }

  s"POST $apiUrl/{name}/{version}/rules" should {
    "return 404" when {
      "when the name+version does not exist" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA")
        datasetFixture.add(datasetV1)

        val response = sendPost[ConformanceRule, String](s"$apiUrl/notFoundDataset/456/rules",
          bodyOpt = Some(LiteralConformanceRule(0, "column1", true, value = "ABC")))
        assertNotFound(response)
      }
    }

    "return 400" when {
      "when the there is a conflicting conf rule #" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", conformance = List(
          LiteralConformanceRule(order = 0, "column1", true, "ABC")
        ))
        datasetFixture.add(datasetV1)

        val response = sendPost[ConformanceRule, String](s"$apiUrl/datasetA/1/rules",
          bodyOpt = Some(LiteralConformanceRule(0, "column1", true, value = "ABC")))
        assertBadRequest(response)

        response.getBody should include("Rule with order 0 cannot be added, another rule with this order already exists.")
      }
      "when rule is not valid (missing MT)" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA")
        datasetFixture.add(datasetV1)

        val response = sendPost[ConformanceRule, Validation](s"$apiUrl/datasetA/1/rules",
          bodyOpt = Some(exampleMcrRule0))
        assertBadRequest(response)

        response.getBody shouldBe Validation.empty.withError("mapping-table", "Mapping table CurrencyMappingTable v9 not found!")
      }
    }

    "return 201" when {
      "when conf rule is added" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", conformance = List(
          LiteralConformanceRule(order = 0, "column1", true, "ABC"))
        )
        datasetFixture.add(datasetV1)

        val response = sendPost[ConformanceRule, Validation](s"$apiUrl/datasetA/1/rules", bodyOpt = Some(exampleLitRule1))
        assertCreated(response)
        // if, in the future, there can be a rule update resulting in a warning, let's reflect that here
        response.getBody shouldBe Validation.empty

        val locationHeader = response.getHeaders.getFirst("location")
        locationHeader should endWith("/api-v3/datasets/datasetA/2/rules/1") // increased version in the url and added rule #1

        val response2 = sendGet[Dataset](s"$apiUrl/datasetA/2")
        assertOk(response2)

        val actual = response2.getBody
        val expectedDsBase = datasetV1.copy(version = 2, parent = Some(DatasetFactory.toParent(datasetV1)),
          conformance = List(datasetV1.conformance.head, exampleLitRule1))
        val expected = toExpected(expectedDsBase, actual)

        assert(actual == expected)
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/rules/{index}" should {
    "return 404" when {
      "when the name+version does not exist" in {
        val response = sendGet[String](s"$apiUrl/notFoundDataset/456/rules/1")
        assertNotFound(response)
      }

      "when the rule with # does not exist" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        datasetFixture.add(dsWithRules1)

        val response = sendGet[String](s"$apiUrl/datasetA/1/rules/345")
        assertNotFound(response)
      }
    }

    "return 200" when {
      "when there is a conformance rule with the order#" in {
        schemaFixture.add(SchemaFactory.getDummySchema("dummySchema"))
        datasetFixture.add(dsWithRules1)

        val response = sendGet[ConformanceRule](s"$apiUrl/datasetA/1/rules/1")
        assertOk(response)
        response.getBody shouldBe LiteralConformanceRule(order = 1, controlCheckpoint = true, outputColumn = "something", value = "1.01")
      }
    }
  }

  // todo add enable cases where dependencies are disabled/removed(?) -> should fail #2065
  // todo add enable cases where dependencies are ok -> should fail #2065

  s"PUT $apiUrl/{name}" can {
    "return 200" when {
      "a Dataset with the given name exists" should {
        "enable the dataset with the given name" in {
          val dsA1 = DatasetFactory.getDummyDataset(name = "dsA", version = 1, disabled = true)
          val dsA2 = DatasetFactory.getDummyDataset(name = "dsA", version = 2, disabled = true)
          val dsB = DatasetFactory.getDummyDataset(name = "dsB", version = 1, disabled = true)
          datasetFixture.add(dsA1, dsA2, dsB)

          val response = sendPut[String, DisabledPayload](s"$apiUrl/dsA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = false)

          // all versions now enabled
          val responseA1 = sendGet[Dataset](s"$apiUrl/dsA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe false

          val responseA2 = sendGet[Dataset](s"$apiUrl/dsA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe false

          // unrelated dataset unaffected
          val responseB = sendGet[Dataset](s"$apiUrl/dsB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe true
        }
      }

      "a Dataset with the given name exists and there have mixed disabled states (historical)" should {
        "enable all versions the dataset with the given name" in {
          val dsA1 = DatasetFactory.getDummyDataset(name = "dsA", version = 1, disabled = true)
          val dsA2 = DatasetFactory.getDummyDataset(name = "dsA", version = 2, disabled = false)
          datasetFixture.add(dsA1, dsA2)

          val response = sendPut[String, DisabledPayload](s"$apiUrl/dsA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = false)

          // all versions enabled
          val responseA1 = sendGet[Dataset](s"$apiUrl/dsA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe false

          val responseA2 = sendGet[Dataset](s"$apiUrl/dsA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe false
        }
      }
    }

    "return 404" when {
      "no Dataset with the given name exists" should {
        "enable nothing" in {
          val response = sendPut[String, DisabledPayload](s"$apiUrl/aDataset")
          assertNotFound(response)
        }
      }
    }
  }

  s"DELETE $apiUrl/{name}" can {
    "return 200" when {
      "a Dataset with the given name exists" should {
        "disable the dataset with the given name" in {
          val dsA1 = DatasetFactory.getDummyDataset(name = "dsA", version = 1)
          val dsA2 = DatasetFactory.getDummyDataset(name = "dsA", version = 2)
          val dsB = DatasetFactory.getDummyDataset(name = "dsB", version = 1)
          datasetFixture.add(dsA1, dsA2, dsB)

          val response = sendDelete[DisabledPayload](s"$apiUrl/dsA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Dataset](s"$apiUrl/dsA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Dataset](s"$apiUrl/dsA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true

          // unrelated dataset unaffected
          val responseB = sendGet[Dataset](s"$apiUrl/dsB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe false
        }
      }

      "a Dataset with the given name exists and there have mixed disabled states (historical)" should {
        "disable all versions the dataset with the given name" in {
          val dsA1 = DatasetFactory.getDummyDataset(name = "dsA", version = 1, disabled = true)
          val dsA2 = DatasetFactory.getDummyDataset(name = "dsA", version = 2, disabled = false)
          datasetFixture.add(dsA1, dsA2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/dsA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Dataset](s"$apiUrl/dsA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Dataset](s"$apiUrl/dsA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
    }

    "return 404" when {
      "no Dataset with the given name exists" should {
        "disable nothing" in {
          val response = sendDelete[String](s"$apiUrl/aDataset")
          assertNotFound(response)
        }
      }
    }
  }
}
