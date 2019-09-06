/*
 * Copyright 2018-2019 ABSA Group Limited
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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.factories.{AttachmentFactory, SchemaFactory}
import za.co.absa.enceladus.menas.integration.fixtures.{AttachmentFixtureService, FixtureService, SchemaFixtureService}
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.models.rest.RestResponse
import za.co.absa.enceladus.menas.models.rest.errors.{SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.model.Schema

import scala.collection.immutable.HashMap

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SchemaApiIntegrationSuite extends BaseRestApiTest {

  @Autowired
  private val schemaFixture: SchemaFixtureService = null

  @Autowired
  private val attachmentFixture: AttachmentFixtureService = null

  private val apiUrl = "/schema"
  private val schemaRefCollection = RefCollection.SCHEMA.name().toLowerCase()

  override def fixtures: List[FixtureService[_]] = List(schemaFixture, attachmentFixture)

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
    }

    "return 400" when {
      "a Schema with that name already exists" in {
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

  s"GET $apiUrl/detail/{name}/latestVersion" should {
    "return 200" when {
      "there is a schema" in {
        val schemaV1 = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        val schemaV2 = SchemaFactory.getDummySchema(name = "schema1",
                                                    version = 2,
                                                    parent = Some(SchemaFactory.toParent(schemaV1)))
        schemaFixture.add(schemaV1)
        schemaFixture.add(schemaV2)

        val response = sendGet[String](s"$apiUrl/detail/schema1/latestVersion")

        assert("2" == response.getBody)
        assertOk(response)
      }
    }

    "return 404" when {
      "there is a schema" in {
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

          val body = response.getBody
          val expected = """|{
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

          val schemaParams = HashMap[String, Any] (
            "name" -> schema.name, "version" -> schema.version, "format" -> "copybook")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", "/test_data/schemas/copybook_ok.cob", schemaParams)
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

          val schemaParams = HashMap[String, Any] (
            "name" -> schema.name, "version" -> schema.version, "format" -> "struct")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", "/test_data/schemas/schema_json_ok.json", schemaParams)
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

          val schemaParams = HashMap[String, Any] (
            "name" -> schema.name, "version" -> schema.version, "format" -> "")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", "/test_data/schemas/schema_json_ok.json", schemaParams)
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

          val schemaParams = HashMap[String, Any] (
            "name" -> schema.name, "version" -> schema.version)
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/upload", "/test_data/schemas/schema_json_ok.json", schemaParams)
          assertCreated(responseUploaded)

          val actual = responseUploaded.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }
    }

    "return 400" when {
      "a copybook with a syntax error" should {
        "return a response containing a schema parsing error with syntax error specific fields" in {
          val schemaParams = HashMap[String, Any] ("version" -> 1, "name" -> "MySchema", "format" -> "copybook")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", "/test_data/schemas/copybook_bogus.cob", schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == "copybook")
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
            s"$apiUrl/upload", "/test_data/schemas/schema_json_bogus.json", schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == "struct")
              assert(body.message.contains("StructType serializer: Failed to convert the JSON string"))
            case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
          }
        }
      }

      "a wrong format has been specified" should {
        "return a response containing a schema format error" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "foo")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/upload", "/test_data/schemas/schema_json_bogus.json", schemaParams)
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
          s"$apiUrl/upload", "/test_data/schemas/copybook_ok.cob", schemaParams)
        assertNotFound(responseUploaded)
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
