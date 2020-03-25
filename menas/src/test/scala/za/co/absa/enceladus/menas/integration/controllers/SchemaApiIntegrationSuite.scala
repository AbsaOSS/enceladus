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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.TestResourcePath
import za.co.absa.enceladus.menas.integration.fixtures._
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.models.rest.RestResponse
import za.co.absa.enceladus.menas.models.rest.errors.{SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.test.factories.{AttachmentFactory, DatasetFactory, MappingTableFactory, SchemaFactory}
import za.co.absa.enceladus.model.{Schema, UsedIn}

import scala.collection.immutable.HashMap

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SchemaApiIntegrationSuite extends BaseRestApiTest with BeforeAndAfterAll {

  private val port = 8877
  private val wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port))

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    wireMockServer.stop()
  }

  @Autowired
  private val schemaFixture: SchemaFixtureService = null

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val mappingTableFixture: MappingTableFixtureService = null

  @Autowired
  private val attachmentFixture: AttachmentFixtureService = null

  private val apiUrl = "/schema"
  private val schemaRefCollection = RefCollection.SCHEMA.name().toLowerCase()

  override def fixtures: List[FixtureService[_]] = List(schemaFixture, attachmentFixture, datasetFixture, mappingTableFixture)

  s"POST $apiUrl/create" can {
    "return 201" when {
      "a Schema is created" should {
        "return the created Schema" in {
          val schema = SchemaFactory.getDummySchema()

          val response = sendPost[Schema, Schema](s"$apiUrl/create", bodyOpt = Some(schema))

          assertCreated(response)

          val actual = response.getBody
          val expected = toExpected(schema, actual)
          assert(actual == expected)
        }
      }
      "all prior versions of the Schema are disabled" should {
        "return the recreated Schema" in {
          val schema = SchemaFactory.getDummySchema(name = "schema", version = 1)
          schemaFixture.add(schema.copy(disabled = true))

          val response = sendPost[Schema, Schema](s"$apiUrl/create", bodyOpt = Some(schema.setVersion(0)))

          assertCreated(response)

          val actual = response.getBody
          val expected = toExpected(schema.setVersion(2), actual).copy(parent = Some(SchemaFactory.toParent(schema)))
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "an enabled Schema with that name already exists" in {
        val schema = SchemaFactory.getDummySchema()
        schemaFixture.add(schema)

        val response = sendPost[Schema, Validation](s"$apiUrl/create", bodyOpt = Some(schema))

        assertBadRequest(response)

        val actual = response.getBody
        val expected = Validation().withError("name", "entity with name already exists: 'dummyName'")
        assert(actual == expected)
      }
    }
  }

  s"POST $apiUrl/edit" can {
    "return 201" when {
      "a Schema with the given name and version is the latest that exists" should {
        "return the updated Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          schemaFixture.add(schema1)

          val response = sendPost[Schema, Schema](s"$apiUrl/edit", bodyOpt = Some(schema1))

          assertCreated(response)

          val actual = response.getBody
          val schema2 = SchemaFactory.getDummySchema(
            name = "schema",
            version = 2,
            parent = Some(SchemaFactory.toParent(schema1)))
          val expected = toExpected(schema2, actual)
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "a Schema with the given name and version exists" should {
        "return the updated Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "Schema", version = 1)
          schemaFixture.add(schema1)
          val schema2 = SchemaFactory.getDummySchema(
            name = "Schema",
            version = 2,
            parent = Some(SchemaFactory.toParent(schema1)))
          schemaFixture.add(schema2)

          val response = sendPost[Schema, Validation](s"$apiUrl/edit", bodyOpt = Some(schema1))
          val expectedValidation = Validation().withError("version", "Version 1 of Schema is not the " +
            "latest version, therefore cannot be edited")
          assertBadRequest(response)
          assert(response.getBody == expectedValidation)
        }
      }
    }
  }

  s"PUT $apiUrl/edit" can {
    "return 201" when {
      "a Schema with the given name and version is the latest that exists" should {
        "return the updated Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          schemaFixture.add(schema1)

          val response = sendPut[Schema, Schema](s"$apiUrl/edit", bodyOpt = Some(schema1))

          assertCreated(response)

          val actual = response.getBody
          val schema2 = SchemaFactory.getDummySchema(
            name = "schema",
            version = 2,
            parent = Some(SchemaFactory.toParent(schema1)))
          val expected = toExpected(schema2, actual)
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "a Schema with the given name and version exists" should {
        "return the updated Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "Schema", version = 1)
          schemaFixture.add(schema1)
          val schema2 = SchemaFactory.getDummySchema(
            name = "Schema",
            version = 2,
            parent = Some(SchemaFactory.toParent(schema1)))
          schemaFixture.add(schema2)

          val response = sendPut[Schema, Validation](s"$apiUrl/edit", bodyOpt = Some(schema1))
          val expectedValidation = Validation().withError("version", "Version 1 of Schema is not the " +
            "latest version, therefore cannot be edited")
          assertBadRequest(response)
          assert(response.getBody == expectedValidation)
        }
      }
    }
  }

  s"DELETE $apiUrl/disable/{name}" can {
    "return 200" when {
      "a Schema with the given name exists" should {
        "disable only the schema with the given name" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "otherSchema", version = 1)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "multiple versions of the Schema with the given name exist" should {
        "disable all versions of the Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "any version of the Schema is only used in disabled Datasets" should {
        "disable all versions of the Schema" in {
          val dataset = DatasetFactory.getDummyDataset(schemaName = "schema", schemaVersion = 1, disabled = true)
          datasetFixture.add(dataset)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "any version of the Schema is only used in disabled MappingTables" should {
        "disable all versions of the Schema" in {
          val mappingTable = MappingTableFactory.getDummyMappingTable(schemaName = "schema", schemaVersion = 1, disabled = true)
          mappingTableFixture.add(mappingTable)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "no Schema with the given name exists" should {
        "disable nothing" in {
          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":0,"modifiedCount":0,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "some version of the Schema is used by an enabled Dataset" should {
        "return a list of the entities the Schema is used in" in {
          val dataset = DatasetFactory.getDummyDataset(name = "dataset", schemaName = "schema", schemaVersion = 1, disabled = false)
          datasetFixture.add(dataset)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, UsedIn](s"$apiUrl/disable/schema")

          assertBadRequest(response)

          val actual = response.getBody
          val expected = UsedIn(Some(Seq(MenasReference(None, "dataset", 1))), Some(Seq()))
          assert(actual == expected)
        }
      }
      "some version of the Schema is used by a enabled MappingTable" should {
        "return a list of the entities the Schema is used in" in {
          val mappingTable = MappingTableFactory.getDummyMappingTable(name = "mapping", schemaName = "schema", schemaVersion = 1, disabled = false)
          mappingTableFixture.add(mappingTable)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, UsedIn](s"$apiUrl/disable/schema")

          assertBadRequest(response)

          val actual = response.getBody
          val expected = UsedIn(Some(Seq()), Some(Seq(MenasReference(None, "mapping", 1))))
          assert(actual == expected)
        }
      }
    }
  }

  s"DELETE $apiUrl/disable/{name}/{version}" can {
    "return 200" when {
      "a Schema with the given name and version exists" should {
        "disable only the schema with the given name and version" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "otherSchema", version = 1)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/1")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "multiple versions of the Schema with the given name exist" should {
        "disable the specified version of the Schema" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/1")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "the version of the Schema is only used in disabled Datasets" should {
        "disable the specified version of the Schema" in {
          val dataset = DatasetFactory.getDummyDataset(schemaName = "schema", schemaVersion = 1, disabled = true)
          datasetFixture.add(dataset)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/1")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "the version of the Schema is not used in enabled Datasets" should {
        "disable the specified version of the Schema" in {
          val dataset = DatasetFactory.getDummyDataset(schemaName = "schema", schemaVersion = 1, disabled = false)
          datasetFixture.add(dataset)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/2")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "the version of the Schema is only used in disabled MappingTables" should {
        "disable the specified version of the Schema" in {
          val mappingTable = MappingTableFactory.getDummyMappingTable(schemaName = "schema", schemaVersion = 1, disabled = true)
          mappingTableFixture.add(mappingTable)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/1")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "the version of the Schema is not used in enabled MappingTables" should {
        "disable the specified version of the Schema" in {
          val mappingTable = MappingTableFactory.getDummyMappingTable(schemaName = "schema", schemaVersion = 1, disabled = false)
          mappingTableFixture.add(mappingTable)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/2")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
      "no Schema with the given name exists" should {
        "disable nothing" in {
          val response = sendDelete[Schema, String](s"$apiUrl/disable/schema/1")

          assertOk(response)

          val actual = response.getBody
          val expected = """{"matchedCount":0,"modifiedCount":0,"upsertedId":null,"modifiedCountAvailable":true}"""
          assert(actual == expected)
        }
      }
    }

    "return 400" when {
      "the version of the Schema is used by an enabled Dataset" should {
        "return a list of the entities the version of the Schema is used in" in {
          val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", schemaName = "schema", schemaVersion = 1, disabled = false)
          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", schemaName = "schema", schemaVersion = 2, disabled = false)
          datasetFixture.add(dataset1, dataset2)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, UsedIn](s"$apiUrl/disable/schema/1")

          assertBadRequest(response)

          val actual = response.getBody
          val expected = UsedIn(Some(Seq(MenasReference(None, "dataset1", 1))), Some(Seq()))
          assert(actual == expected)
        }
      }
      "some version of the Schema is used by a enabled MappingTable" should {
        "return a list of the entities the version of the Schema is used in" in {
          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mapping1", schemaName = "schema", schemaVersion = 1, disabled = false)
          val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mapping2", schemaName = "schema", schemaVersion = 2, disabled = false)
          mappingTableFixture.add(mappingTable1, mappingTable2)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[Schema, UsedIn](s"$apiUrl/disable/schema/1")

          assertBadRequest(response)

          val actual = response.getBody
          val expected = UsedIn(Some(Seq()), Some(Seq(MenasReference(None, "mapping1", 1))))
          assert(actual == expected)
        }
      }
    }
  }

  s"GET $apiUrl/detail/{name}/latestVersion" should {
    "return 200" when {
      "a Schema with the given name exists" in {
        val schemaV1 = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        val schemaV2 = SchemaFactory.getDummySchema(name = "schema1",
          version = 2,
          parent = Some(SchemaFactory.toParent(schemaV1)))
        schemaFixture.add(schemaV1)
        schemaFixture.add(schemaV2)

        val response = sendGet[String](s"$apiUrl/detail/schema1/latestVersion")

        assertOk(response)
        assert("2" == response.getBody)
      }
    }

    "return 404" when {
      "a Schema with the given name does not exist" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/detail/schema2/latestVersion")

        assertNotFound(response)
      }
    }
  }

  s"GET $apiUrl/json/{name}/{version}" should {
    "return 404" when {
      "no schema exists for the specified name" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/json/otherSchemaName/1")

        assertNotFound(response)
      }
      "no schema exists for the specified version" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/json/schema1/2")

        assertNotFound(response)
      }
      "the schema has no fields" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/json/schema1/1")

        assertNotFound(response)
      }
      "a non-boolean value is provided for the `pretty` query parameter" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/json/schema1/1?pretty=tru")

        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a Schema with the specified name and version" should {
        "return the Spark Struct representation of a schema as a JSON (pretty=false by default)" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/json/schema1/1")

          assertOk(response)

          val body = response.getBody
          val expected = """{"type":"struct","fields":[{"name":"dummyFieldName","type":"string","nullable":true,"metadata":{}}]}"""
          assert(body == expected)
        }
        "return the Spark Struct representation of a schema as a JSON (pretty=false explict)" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/json/schema1/1?pretty=false")

          assertOk(response)

          val body = response.getBody
          val expected = """{"type":"struct","fields":[{"name":"dummyFieldName","type":"string","nullable":true,"metadata":{}}]}"""
          assert(body == expected)
        }
        "return the Spark Struct representation of a schema as a pretty JSON" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/json/schema1/1?pretty=true")

          assertOk(response)

          val body = response.getBody.replace("\r\n", "\n") // this will make it work on Windows (CRLF->LF), too.
          val expected =
            """|{
               |  "type" : "struct",
               |  "fields" : [ {
               |    "name" : "dummyFieldName",
               |    "type" : "string",
               |    "nullable" : true,
               |    "metadata" : { }
               |  } ]
               |}""".stripMargin
          assert(body == expected)
        }
      }
    }
  }

  s"GET $apiUrl/export/{name}/{version}" should {
    "return 404" when {
      "no Attachment exists for the specified name" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val response = sendGet[Array[Byte]](s"$apiUrl/export/otherSchemaName/2")

        assertNotFound(response)
        assert(!response.getHeaders.containsKey("mime-type"))
      }
      "no Attachment exists with a version up to the specified version" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val response = sendGet[Array[Byte]](s"$apiUrl/export/schemaName/1")

        assertNotFound(response)
        assert(!response.getHeaders.containsKey("mime-type"))
      }
    }

    "return 200" when {
      val attachment1 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 1,
        refCollection = schemaRefCollection,
        fileContent = Array(1, 2, 3))
      val attachment2 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 2,
        refCollection = schemaRefCollection,
        fileContent = Array(2, 3, 4),
        fileMIMEType = "application/octet-stream")
      val attachment4 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 4,
        refCollection = schemaRefCollection,
        fileContent = Array(4, 5, 6),
        fileMIMEType = "application/json")
      val attachment5 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 5,
        refCollection = schemaRefCollection,
        fileContent = Array(5, 6, 7))
      "there are Attachments with previous and subsequent versions" should {
        "return the byte array of the uploaded file with the nearest previous version" in {
          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

          val response = sendGet[Array[Byte]](s"$apiUrl/export/schemaName/3")

          assertOk(response)
          assert(response.getHeaders.containsKey("mime-type"))
          assert(response.getHeaders.get("mime-type").get(0) == "application/octet-stream")

          val body = response.getBody
          assert(body.sameElements(attachment2.fileContent))
        }
      }
      "there is an Attachment with the exact version" should {
        "return the byte array of the uploaded file with the exact version" in {
          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

          val response = sendGet[Array[Byte]](s"$apiUrl/export/schemaName/4")

          assertOk(response)
          assert(response.getHeaders.containsKey("mime-type"))
          assert(response.getHeaders.get("mime-type").get(0) == "application/json")

          val body = response.getBody
          assert(body.sameElements(attachment4.fileContent))
        }
      }
    }
  }

  s"POST $apiUrl/upload" should {
    "return 201" when {
      "a copybook has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "copybook")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", TestResourcePath.Copybook.ok, schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 3)
        }
      }

      "a JSON struct type schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "struct")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", TestResourcePath.Json.ok, schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }

      "a JSON struct type schema is uploaded, but an empty format type is specified" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", TestResourcePath.Json.ok, schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }

      "a JSON struct type schema is uploaded, but no format type is specified" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version)
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", TestResourcePath.Json.ok, schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }

      "an avro schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "avro")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", TestResourcePath.Avro.ok, schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 7)
        }
      }
    }

    "return 400" when {
      "a copybook with a syntax error" should {
        "return a response containing a schema parsing error with syntax error specific fields" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "copybook")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", TestResourcePath.Copybook.bogus, schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == SchemaType.Copybook)
              assert(e.line.contains(22))
              assert(e.field.contains("B1"))
              assert(body.message.contains("Syntax error in the copybook"))
            case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
          }
        }
      }

      "a JSON struct type schema with a syntax error" should {
        "return a response containing a schema parsing error returned by the StructType parser" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "struct")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", TestResourcePath.Json.bogus, schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == SchemaType.Struct)
              assert(body.message.contains("StructType serializer: Failed to convert the JSON string"))
            case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
          }
        }
      }

      "an avro-schema with a syntax error" should {
        "return a response containing a schema parsing error encountered during avro schema parsing" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "avro")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", TestResourcePath.Avro.bogus, schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == SchemaType.Avro)
              assert(body.message.contains("Record has no fields"))
            case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
          }
        }
      }

      "a wrong format has been specified" should {
        "return a response containing a schema format error" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "foo")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", TestResourcePath.Json.bogus, schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaFormatError) =>
              assert(e.errorType == "schema_format")
              assert(e.schemaType == "foo")
              assert(body.message.contains("'foo' is not a recognized schema format."))
            case e => fail(s"Expected an instance of SchemaFormatError, got $e.")
          }
        }
      }
    }

    "return 404" when {
      "a schema file is uploaded, but no schema exists for the specified name and version" in {
        val schemaParams = HashMap[String, Any](
          "name" -> "dummy", "version" -> 1, "format" -> "copybook")
        val responseUploaded = sendPostUploadFile[Schema](
          s"$apiUrl/upload", TestResourcePath.Copybook.ok, schemaParams)
        assertNotFound(responseUploaded)
      }
    }
  }

  def readTestResourceAsString(path: String): String = IOUtils.toString(getClass.getResourceAsStream(path))

  s"POST $apiUrl/remote" should {
    import com.github.tomakehurst.wiremock.client.WireMock._

    val remoteFilePath = "/remote-test/someRemoteFile.ext"
    val remoteUrl = s"http://localhost:$port$remoteFilePath"

    "return 201" when {
      "a copybook has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(ok(readTestResourceAsString(TestResourcePath.Copybook.ok))))

          val params = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "copybook", "remoteUrl" -> remoteUrl) // TODO string value from somewhere?
          val responseRemoteLoaded = sendPostRemoteFile[Schema](s"$apiUrl/remote", params)
          assertCreated(responseRemoteLoaded)

          val actual = responseRemoteLoaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 3)
        }
      }

      Seq(
        ("explicit JSON struct format", Some("format" -> "struct")),
        ("implicit JSON struct (by empty string format)", Some("format" -> "")),
        ("implicit JSON struct (by no format at all)", None)
      ).foreach { case (name, formatSpec) =>
        s"an $name schema has no errors" should {
          "return a new version of the schema" in {
            val schema = SchemaFactory.getDummySchema()
            schemaFixture.add(schema)

            wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
              .willReturn(ok(readTestResourceAsString(TestResourcePath.Json.ok))))

            // conditionally adding ("format" -> "struct"/"") or no format at all
            val params = HashMap("name" -> schema.name, "version" -> schema.version, "remoteUrl" -> remoteUrl) ++ formatSpec
            val responseRemoteLoaded = sendPostRemoteFile[Schema](s"$apiUrl/remote", params)
            assertCreated(responseRemoteLoaded)

            val actual = responseRemoteLoaded.getBody
            assert(actual.name == schema.name)
            assert(actual.version == schema.version + 1)
            assert(actual.fields.length == 2)
          }
        }
      }

      "an avro schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema()
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(ok(readTestResourceAsString(TestResourcePath.Avro.ok))))

          val params = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "avro", "remoteUrl" -> remoteUrl)
          val responseRemoteLoaded = sendPostRemoteFile[Schema](s"$apiUrl/remote", params)
          assertCreated(responseRemoteLoaded)

          val actual = responseRemoteLoaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 7)
        }
      }
    }

    "return 400" when {
      Seq(
        (SchemaType.Copybook, TestResourcePath.Copybook.bogus, "Syntax error in the copybook"),
        (SchemaType.Struct, TestResourcePath.Json.bogus, "StructType serializer: Failed to convert the JSON string"),
        (SchemaType.Avro, TestResourcePath.Avro.bogus, "Record has no fields")
      ).foreach { case (schemaType, testResourcePath, expectedErrorMessage) =>

        s"a $schemaType with a syntax error" should {
          "return a response containing a schema parsing error with syntax error specific fields" in {
            wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
              .willReturn(ok(readTestResourceAsString(testResourcePath))))

            val params = HashMap("name" -> "MySchema", "version" -> 1, "format" -> schemaType.toString, "remoteUrl" -> remoteUrl)
            val response = sendPostRemoteFile[RestResponse](s"$apiUrl/remote", params)
            val body = response.getBody

            assertBadRequest(response)
            body.error match {
              case Some(e: SchemaParsingError) =>
                assert(e.errorType == "schema_parsing")
                assert(e.schemaType == schemaType)
                assert(body.message.contains(expectedErrorMessage))
              case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
            }
          }
        }
      }

      "a wrong format has been specified" should {
        "return a response containing a schema format error" in {
          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(ok(readTestResourceAsString(TestResourcePath.Json.ok))))

          val params = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "foo", "remoteUrl" -> remoteUrl)
          val response = sendPostRemoteFile[RestResponse](s"$apiUrl/remote", params)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaFormatError) =>
              assert(e.errorType == "schema_format")
              assert(e.schemaType == "foo")
              assert(body.message.contains("'foo' is not a recognized schema format."))
            case e => fail(s"Expected an instance of SchemaFormatError, got $e.")
          }
        }
      }
    }

    "return 404" when {
      "a schema file is loaded from remote url, but no schema exists for the specified name and version" in {
        wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
          .willReturn(ok(readTestResourceAsString(TestResourcePath.Copybook.ok))))

        val params = HashMap[String, Any]("version" -> 1, "name" -> "dummy", "format" -> "copybook", "remoteUrl" -> remoteUrl)
        val response = sendPostRemoteFile[Schema](s"$apiUrl/remote", params)
        assertNotFound(response)
      }
    }
  }

  private def toExpected(schema: Schema, actual: Schema): Schema = {
    schema.copy(
      dateCreated = actual.dateCreated,
      userCreated = actual.userCreated,
      lastUpdated = actual.lastUpdated,
      userUpdated = actual.userUpdated,
      dateDisabled = actual.dateDisabled,
      userDisabled = actual.userDisabled)
  }
}
