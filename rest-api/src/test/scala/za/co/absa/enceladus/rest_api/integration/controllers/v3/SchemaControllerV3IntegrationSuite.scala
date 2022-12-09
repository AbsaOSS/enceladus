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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.test.factories.{AttachmentFactory, DatasetFactory, MappingTableFactory, SchemaFactory}
import za.co.absa.enceladus.model.versionedModel.NamedVersion
import za.co.absa.enceladus.model.{Schema, SchemaField, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.TestResourcePath
import za.co.absa.enceladus.rest_api.exceptions.EntityInUseException
import za.co.absa.enceladus.rest_api.integration.controllers.TestPaginatedMatchers.conformTo
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.models.rest.errors.{SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.rest_api.models.rest.{DisabledPayload, Paginated, RestResponse}
import za.co.absa.enceladus.rest_api.repositories.RefCollection
import za.co.absa.enceladus.rest_api.utils.SchemaType

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Path}
import scala.collection.immutable.HashMap

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class SchemaControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  private val port = 8877 // same  port as in test/resources/application.conf in the `enceladus.rest.schemaRegistry.baseUrl` key
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

  private val apiUrl = "/schemas"
  private val schemaRefCollection = RefCollection.SCHEMA.name().toLowerCase()

  override def fixtures: List[FixtureService[_]] = List(schemaFixture, attachmentFixture, datasetFixture, mappingTableFixture)

  // scalastyle:off magic.number - the test deliberately contains test actual data (no need for DRY here)
  s"GET $apiUrl" should {
    "return 200" when {
      "paginated schema by default params (offset=0, limit=20)" in {
        val schemasA = (1 to 15).map(i => SchemaFactory.getDummySchema(name = "schemaA", version = i))
        val schemaA2disabled = SchemaFactory.getDummySchema(name = "schemaA2", version = 1, disabled = true) // skipped in listing
        val schemasB2M = ('B' to 'V').map(suffix => SchemaFactory.getDummySchema(name = s"schema$suffix"))
        schemaFixture.add(schemasA: _*)
        schemaFixture.add(schemaA2disabled)
        schemaFixture.add(schemasB2M: _*)

        val response = sendGet[TestPaginatedNamedVersion](s"$apiUrl")
        assertOk(response)
        response.getBody should conformTo(Paginated(offset = 0, limit = 20, truncated = true, page = Seq(
          NamedVersion("schemaA", 15), NamedVersion("schemaB", 1), NamedVersion("schemaC", 1), NamedVersion("schemaD", 1), NamedVersion("schemaE", 1),
          NamedVersion("schemaF", 1), NamedVersion("schemaG", 1), NamedVersion("schemaH", 1), NamedVersion("schemaI", 1), NamedVersion("schemaJ", 1),
          NamedVersion("schemaK", 1), NamedVersion("schemaL", 1), NamedVersion("schemaM", 1), NamedVersion("schemaN", 1), NamedVersion("schemaO", 1),
          NamedVersion("schemaP", 1), NamedVersion("schemaQ", 1), NamedVersion("schemaR", 1), NamedVersion("schemaS", 1), NamedVersion("schemaT", 1)
          // U, V are on the page 2
        )).asTestPaginated)
      }

      "paginated schemas with custom pagination (offset=10, limit=5)" in {
        val schemasA2o = ('A' to 'O').map(suffix => SchemaFactory.getDummySchema(name = s"schema$suffix"))
        schemaFixture.add(schemasA2o: _*)

        val response = sendGet[TestPaginatedNamedVersion](s"$apiUrl?offset=10&limit=5")
        assertOk(response)
        response.getBody should conformTo(Paginated(offset = 10, limit = 5, truncated = false, page = Seq(
          // A-E = page 1
          // F-J = page 2
          NamedVersion("schemaK", 1), NamedVersion("schemaL", 1), NamedVersion("schemaM", 1), NamedVersion("schemaN", 1), NamedVersion("schemaO", 1)
          // no truncation
        )).asTestPaginated)
      }
      "paginated schemas as serialized string" in {
        val schemasA2o = ('A' to 'D').map(suffix => SchemaFactory.getDummySchema(name = s"schema$suffix"))
        schemaFixture.add(schemasA2o: _*)

        val response = sendGet[String](s"$apiUrl?limit=3")
        assertOk(response)
        response.getBody shouldBe
          """{"page":[
            |{"name":"schemaA","version":1,"disabled":false},
            |{"name":"schemaB","version":1,"disabled":false},
            |{"name":"schemaC","version":1,"disabled":false}
            |],
            |"offset":0,
            |"limit":3,
            |"truncated":true
            |}
            |""".stripMargin.replace("\n", "")
      }
    }
  }

  s"POST $apiUrl" can {
    "return 201" when {
      "a Schema is created (v1-payload has defined fields already)" in {
        val schema = SchemaFactory.getDummySchema("schemaA", fields = List(
          SchemaField("field1", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)
        ))

        val response = sendPost[Schema, Validation](apiUrl, bodyOpt = Some(schema))

        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/schemas/schemaA/1")

        response.getBody shouldBe Validation.empty

        val response2 = sendGet[Schema]("/schemas/schemaA/1")
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(schema, actual)
        assert(actual == expected)
      }
    }

    "return 400" when {
      "a Schema is created (empty fields = warning)" in {
        val schema = SchemaFactory.getDummySchema("schemaA")
        val response = sendPost[Schema, Validation](apiUrl, bodyOpt = Some(schema))

        assertBadRequest(response)
        response.getBody shouldBe Validation.empty
          .withError("schema-fields", "No fields found! There must be fields defined for actual usage.")
      }
      "an enabled Schema with that name already exists" in {
        val schema = SchemaFactory.getDummySchema(fields = List(
          SchemaField("field1", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)))
        schemaFixture.add(schema)

        val response = sendPost[Schema, Validation](apiUrl, bodyOpt = Some(schema))

        assertBadRequest(response)

        val actual = response.getBody
        val expected = Validation.empty
          .withError("name", "entity with name already exists: 'dummyName'")

        assert(actual == expected)
      }
    }
  }

  s"PUT $apiUrl/{name}/{version}" can {
    "return 201" when {
      "a Schema is updated (v1-payload has defined fields already)" in {
        val schema1 = SchemaFactory.getDummySchema("schemaA", fields = List(
          SchemaField("field1", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)
        ))
        schemaFixture.add(schema1)

        val schema2 = SchemaFactory.getDummySchema("schemaA", fields = List(
          SchemaField("anotherField", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)
        ))
        val response = sendPut[Schema, Validation](s"$apiUrl/schemaA/1", bodyOpt = Some(schema2))

        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("Location")
        locationHeader should endWith("/api-v3/schemas/schemaA/2")

        response.getBody shouldBe Validation.empty

        val response2 = sendGet[Schema]("/schemas/schemaA/2")
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(schema2.copy(version = 2, parent = Some(SchemaFactory.toParent(schema1))), actual)
        assert(actual == expected)
      }
    }

    "return 400" when {
      "a Schema fails to update due to empty fields" in {
        val schema1 = SchemaFactory.getDummySchema("schemaA")
        schemaFixture.add(schema1)

        val schema2 = SchemaFactory.getDummySchema("schemaA")
        val response = sendPut[Schema, Validation](s"$apiUrl/schemaA/1", bodyOpt = Some(schema2))

        assertBadRequest(response)
        response.getBody shouldBe Validation.empty
          .withError("schema-fields", "No fields found! There must be fields defined for actual usage.")
      }
      "schema is disabled" in {
        val schema1 = SchemaFactory.getDummySchema("schemaA", disabled = true, fields = List(
          SchemaField("field1", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)
        ))
        schemaFixture.add(schema1)

        val schema2 = SchemaFactory.getDummySchema("schemaA", fields = List(
          SchemaField("anotherField", "string", "", nullable = true, metadata = Map.empty, children = Seq.empty)
        ))
        val response = sendPut[Schema, Validation](s"$apiUrl/schemaA/1", bodyOpt = Some(schema2))

        assertBadRequest(response)
        response.getBody shouldBe Validation.empty.withError("disabled", "Entity schemaA is disabled!")
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/json" should {
    "return 404" when {
      "no schema exists for the specified name" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/otherSchemaName/1/json")

        assertNotFound(response)
      }
      "no schema exists for the specified version" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/schema1/2/json")

        assertNotFound(response)
      }
      "the schema has no fields" in {
        val schema = SchemaFactory.getDummySchema(name = "schemaA", version = 1)
        schemaFixture.add(schema)

        val response = sendGet[Validation](s"$apiUrl/schemaA/1/json")
        assertBadRequest(response)
        response.getBody shouldBe
          Validation.empty.withError("schema-fields", "Schema schemaA v1 exists, but has no fields!")

      }
      "a non-boolean value is provided for the `pretty` query parameter" in {
        val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
        schemaFixture.add(schema)

        val response = sendGet[String](s"$apiUrl/schema1/1/json?pretty=tru")

        assertNotFound(response)
      }
    }

    "return 200" when {
      "there is a Schema with the specified name and version" should {
        "return the Spark Struct representation of a schema as a JSON (pretty=false by default)" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/schema1/1/json")

          assertOk(response)

          val body = response.getBody
          val expected = """{"type":"struct","fields":[{"name":"dummyFieldName","type":"string","nullable":true,"metadata":{}}]}"""
          assert(body == expected)
        }
        "return the Spark Struct representation of a schema as a JSON (pretty=false explict)" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/schema1/1/json?pretty=false")

          assertOk(response)

          val body = response.getBody
          val expected = """{"type":"struct","fields":[{"name":"dummyFieldName","type":"string","nullable":true,"metadata":{}}]}"""
          assert(body == expected)
        }
        "return the Spark Struct representation of a schema as a pretty JSON" in {
          val schema = SchemaFactory.getDummySchema(name = "schema1", version = 1, fields = List(SchemaFactory.getDummySchemaField()))
          schemaFixture.add(schema)

          val response = sendGet[String](s"$apiUrl/schema1/1/json?pretty=true")

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

  s"GET $apiUrl/{name}/{version}/original" should {
    "return 404" when {
      "no Attachment exists for the specified name" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val response = sendGet[Array[Byte]](s"$apiUrl/otherSchemaName/2/original")

        assertNotFound(response)
        assert(!response.getHeaders.containsKey("mime-type"))
      }
      "no Attachment exists with a version up to the specified version" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val response = sendGet[Array[Byte]](s"$apiUrl/schemaName/1/original")

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
        fileMIMEType = MediaType.APPLICATION_OCTET_STREAM_VALUE)
      val attachment4 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 4,
        refCollection = schemaRefCollection,
        fileContent = Array(4, 5, 6),
        fileMIMEType = MediaType.APPLICATION_JSON_VALUE)
      val attachment5 = AttachmentFactory.getDummyAttachment(
        refName = "schemaName",
        refVersion = 5,
        refCollection = schemaRefCollection,
        fileContent = Array(5, 6, 7))
      "there are Attachments with previous and subsequent versions" should {
        "return the byte array of the uploaded file with the nearest previous version" in {
          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

          val response = sendGet[Array[Byte]](s"$apiUrl/schemaName/3/original")

          assertOk(response)
          assert(response.getHeaders.containsKey("mime-type"))
          assert(response.getHeaders.get("mime-type").get(0) == MediaType.APPLICATION_OCTET_STREAM_VALUE)

          val body = response.getBody
          assert(body.sameElements(attachment2.fileContent))
        }
      }
      "there is an Attachment with the exact version" should {
        "return the byte array of the uploaded file with the exact version" in {
          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

          val response = sendGet[Array[Byte]](s"$apiUrl/schemaName/4/original")

          assertOk(response)
          assert(response.getHeaders.containsKey("mime-type"))
          assert(response.getHeaders.get("mime-type").get(0) == "application/json")

          val body = response.getBody
          assert(body.sameElements(attachment4.fileContent))
        }
      }
    }
  }

  s"POST $apiUrl/{name}/{version}/from-file" should {
    "return 201" when {
      "a copybook has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA", version = 1)
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, String]("format" -> "copybook")
          val responseUploaded = sendPostUploadFile[Validation](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Copybook.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 3)
        }
      }

      "a JSON struct type schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any]("format" -> "struct")
          val responseUploaded = sendPostUploadFile[Validation](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }

      "an avro schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          val schemaParams = HashMap[String, Any]("format" -> "avro")
          val responseUploaded = sendPostUploadFile[Schema](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Avro.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 7)
        }
      }
    }

    "return 400" when {
      "no upload format type is specified" in {
        val schema = SchemaFactory.getDummySchema("schemaA")
        schemaFixture.add(schema)

        val schemaParams = HashMap.empty[String, Any] // v2 fallbacked on this, v3 forbids it
        val response = sendPostUploadFile[String](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
        assertBadRequest(response)
        response.getBody should include("Required String parameter 'format' is not present")
      }

      "an empty upload format type is specified" in {
        val schema = SchemaFactory.getDummySchema("schemaA")
        schemaFixture.add(schema)

        val schemaParams = HashMap[String, Any]("format" -> "") // v2 fallbacked on this, v3 forbids it
        val response = sendPostUploadFile[String](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
        assertBadRequest(response)
        response.getBody should include("not a recognized schema format. Enceladus currently supports: struct, copybook, avro.")
      }

      "a copybook with a syntax error" should {
        "return a response containing a schema parsing error with syntax error specific fields" in {
          val schemaParams = HashMap[String, Any]("format" -> "copybook")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Copybook.bogus, schemaParams)
          val body = response.getBody

          assertBadRequest(response)
          body.error match {
            case Some(e: SchemaParsingError) =>
              assert(e.errorType == "schema_parsing")
              assert(e.schemaType == SchemaType.Copybook)
              assert(e.line.contains(22))
              assert(e.field.contains(""))
              assert(body.message.contains("Syntax error in the copybook"))
            case e => fail(s"Expected an instance of SchemaParsingError, got $e.")
          }
        }
      }

      "a JSON struct type schema with a syntax error" should {
        "return a response containing a schema parsing error returned by the StructType parser" in {
          val schemaParams = HashMap[String, Any]("format" -> "struct")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.bogus, schemaParams)
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
          val schemaParams = HashMap[String, Any]("format" -> "avro")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Avro.bogus, schemaParams)
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
          val schemaParams = HashMap[String, Any]("format" -> "foo")
          val response = sendPostUploadFile[RestResponse](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.bogus, schemaParams)
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
      "schema is disabled" in {
        val schema = SchemaFactory.getDummySchema("schemaA", disabled = true)
        schemaFixture.add(schema)

        val schemaParams = HashMap[String, Any]("format" -> "struct")
        val responseUploaded = sendPostUploadFile[Validation](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
        assertBadRequest(responseUploaded)
        responseUploaded.getBody shouldBe Validation.empty.withError("disabled", "Entity schemaA is disabled!")
      }
    }

    "return 404" when {
      "a schema file is uploaded, but no schema exists for the specified name and version" in {
        val schemaParams = HashMap[String, Any]("format" -> "copybook")
        val responseUploaded = sendPostUploadFile[Schema](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Copybook.ok, schemaParams)
        assertNotFound(responseUploaded)
      }
    }
  }

  import com.github.tomakehurst.wiremock.client.WireMock._

  private def readTestResourceAsString(path: String): String =
    IOUtils.toString(getClass.getResourceAsStream(path), Charset.defaultCharset())

  /**
   * will prepare the a response from file with correct `ContentType`
   */
  private def readTestResourceAsResponseWithContentType(path: String): ResponseDefinitionBuilder = {
    // this is crazy, but it works better than hardcoding mime-types
    val filePath: Path = new File(getClass.getResource(path).toURI).toPath
    val mime = Option(Files.probeContentType(filePath)).getOrElse(MediaType.APPLICATION_OCTET_STREAM_VALUE) // default for e.g. cob

    val content = readTestResourceAsString(path)
    import com.github.tomakehurst.wiremock.client.WireMock._
    okForContentType(mime, content)
  }

  s"POST $apiUrl/{name}/{version}/from-remote-uri" should {

    val remoteFilePath = "/remote-test/someRemoteFile.ext"
    val remoteUrl = s"http://localhost:$port$remoteFilePath"

    "return 201" when {
      "a copybook has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Copybook.ok)))

          val params = HashMap[String, Any]("format" -> "copybook", "remoteUrl" -> remoteUrl)
          val responseRemoteLoaded = sendPostRemoteFile[Validation](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          responseRemoteLoaded.getBody shouldBe Validation.empty

          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 3)
        }
      }

      s"struct schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Json.ok)))

          val params = HashMap("remoteUrl" -> remoteUrl, "format" -> "struct")
          val responseRemoteLoaded = sendPostRemoteFile[Validation](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          responseRemoteLoaded.getBody shouldBe Validation.empty
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 2)
        }
      }

      "an avro schema has no errors" should {
        "return a new version of the schema" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))

          val params = HashMap[String, Any]("format" -> "avro", "remoteUrl" -> remoteUrl)
          val responseRemoteLoaded = sendPostRemoteFile[Validation](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          responseRemoteLoaded.getBody shouldBe Validation.empty

          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
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
              .willReturn(readTestResourceAsResponseWithContentType(testResourcePath)))

            val params = HashMap("format" -> schemaType.toString, "remoteUrl" -> remoteUrl)
            val response = sendPostRemoteFile[RestResponse](s"$apiUrl/schemaA/1/from-remote-uri", params)
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
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Json.ok)))

          val params = HashMap[String, Any]("format" -> "foo", "remoteUrl" -> remoteUrl)
          val response = sendPostRemoteFile[RestResponse](s"$apiUrl/schemaA/1/from-remote-uri", params)
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
      "the schema is disabled" in {
        val schema = SchemaFactory.getDummySchema("schemaA", disabled = true)
        schemaFixture.add(schema)

        wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
          .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))

        val params = HashMap[String, Any]("format" -> "avro", "remoteUrl" -> remoteUrl)
        val responseRemoteLoaded = sendPostRemoteFile[Validation](s"$apiUrl/schemaA/1/from-remote-uri", params)
        assertBadRequest(responseRemoteLoaded)
        responseRemoteLoaded.getBody shouldBe Validation.empty.withError("disabled", "Entity schemaA is disabled!")

      }
    }

    "return 404" when {
      "a schema file is loaded from remote url, but no schema exists for the specified name and version" in {
        wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
          .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Copybook.ok)))

        val params = HashMap[String, Any]("format" -> "copybook", "remoteUrl" -> remoteUrl)
        val response = sendPostRemoteFile[Schema](s"$apiUrl/schemaA/1/from-remote-uri", params)
        assertNotFound(response)
      }
    }
  }

  s"POST $apiUrl/{name}/{version}/from-registry" should {
    def subjectPath(subjectName: String) = s"/subjects/$subjectName/versions/latest/schema"

    "return 201" when {
      "an avro schema has no errors" should {
        "load schema by subject name as-is" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic1-value")))
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))

          val params = HashMap[String, Any]("format" -> "avro", "subject" -> "myTopic1-value")
          val responseRemoteLoaded = sendPostSubject[Validation](s"$apiUrl/schemaA/1/from-registry", params)
          assertCreated(responseRemoteLoaded)
          responseRemoteLoaded.getBody shouldBe Validation.empty

          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 7)
        }

        "load schema by subject name -value fallback" in {
          val schema = SchemaFactory.getDummySchema("schemaA")
          schemaFixture.add(schema)

          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic2"))) // will fail
            .willReturn(notFound()))

          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic2-value"))) // fallback will kick in
            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))

          val params = HashMap[String, Any]("format" -> "avro", "subject" -> "myTopic2")
          val responseRemoteLoaded = sendPostSubject[Validation](s"$apiUrl/schemaA/1/from-registry", params)
          assertCreated(responseRemoteLoaded)
          responseRemoteLoaded.getBody shouldBe Validation.empty

          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("Location")
          locationHeader should endWith("/api-v3/schemas/schemaA/2") // +1 version

          val response2 = sendGet[Schema]("/schemas/schemaA/2")
          assertOk(response2)

          val actual = response2.getBody
          assert(actual.name == schema.name)
          assert(actual.version == schema.version + 1)
          assert(actual.fields.length == 7)
        }
      }
    }

    "return 400" when {
      "the schema is disabled" in {
        val schema = SchemaFactory.getDummySchema("schemaA", disabled = true)
        schemaFixture.add(schema)

        wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic1-value")))
          .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))

        val params = HashMap[String, Any]("format" -> "avro", "subject" -> "myTopic1-value")
        val responseRemoteLoaded = sendPostSubject[Validation](s"$apiUrl/schemaA/1/from-registry", params)
        assertBadRequest(responseRemoteLoaded)
        responseRemoteLoaded.getBody shouldBe Validation.empty.withError("disabled", "Entity schemaA is disabled!")
      }
    }
  }

  s"GET $apiUrl/{name}/used-in" should {
    "return 200" when {
      "there are used-in records" in {
        val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
        val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
        schemaFixture.add(schema1, schema2)

        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA", schemaName = "schema", schemaVersion = 1)
        val datasetB = DatasetFactory.getDummyDataset(name = "datasetB", schemaName = "schema", schemaVersion = 1, disabled = true)
        datasetFixture.add(datasetA, datasetB)

        val mappingTableA = MappingTableFactory.getDummyMappingTable(name = "mappingA", schemaName = "schema", schemaVersion = 1)
        val mappingTableB = MappingTableFactory.getDummyMappingTable(name = "mappingB", schemaName = "schema", schemaVersion = 1, disabled = true)
        val mappingTableC = MappingTableFactory.getDummyMappingTable(name = "mappingC", schemaName = "schema", schemaVersion = 2)
        mappingTableFixture.add(mappingTableA, mappingTableB, mappingTableC)


        val response = sendGet[UsedIn](s"$apiUrl/schema/used-in")
        assertOk(response)

        // datasetB and mappingB are disabled -> not reported
        // mappingC is reported, even though it schema is tied to schema-v2, because disabling is done on the whole entity in API v3
        response.getBody shouldBe UsedIn(
          datasets = Some(Seq(Reference(None, "datasetA", 1))),
          mappingTables = Some(Seq(Reference(None, "mappingA", 1), Reference(None, "mappingC", 1)))
        )
      }
    }
  }

  s"GET $apiUrl/{name}/{version}/used-in" should {
    "return 200" when {
      "there are used-in records for particular version" in {
        val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
        val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
        schemaFixture.add(schema1, schema2)

        val datasetA = DatasetFactory.getDummyDataset(name = "datasetA", schemaName = "schema", schemaVersion = 1)
        val datasetB = DatasetFactory.getDummyDataset(name = "datasetB", schemaName = "schema", schemaVersion = 1, disabled = true)
        datasetFixture.add(datasetA, datasetB)

        val mappingTableA = MappingTableFactory.getDummyMappingTable(name = "mappingA", schemaName = "schema", schemaVersion = 1)
        val mappingTableB = MappingTableFactory.getDummyMappingTable(name = "mappingB", schemaName = "schema", schemaVersion = 1, disabled = true)
        val mappingTableC = MappingTableFactory.getDummyMappingTable(name = "mappingC", schemaName = "schema", schemaVersion = 2)
        mappingTableFixture.add(mappingTableA, mappingTableB, mappingTableC)


        val response = sendGet[UsedIn](s"$apiUrl/schema/1/used-in")
        assertOk(response)

        // datasetB and mappingB are disabled -> not reported
        // mappingC is tied to schema v2 -> not reported
        response.getBody shouldBe UsedIn(
          datasets = Some(Seq(Reference(None, "datasetA", 1))),
          mappingTables = Some(Seq(Reference(None, "mappingA", 1)))
        )
      }
    }
  }

  s"PUT $apiUrl/{name}" can {
    "return 200" when {
      "a Schema with the given name exists" should {
        "enable the Schema with the given name" in {
          val schA1 = SchemaFactory.getDummySchema(name = "schA", version = 1, disabled = true)
          val schA2 = SchemaFactory.getDummySchema(name = "schA", version = 2, disabled = true)
          val schB = SchemaFactory.getDummySchema(name = "schB", version = 1, disabled = true)
          schemaFixture.add(schA1, schA2, schB)

          val response = sendPut[String, DisabledPayload](s"$apiUrl/schA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = false)

          // all versions now enabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe false

          val responseA2 = sendGet[Schema](s"$apiUrl/schA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe false

          // unrelated schema unaffected
          val responseB = sendGet[Schema](s"$apiUrl/schB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe true
        }
      }

      "a Schema with the given name exists and there have mixed disabled states (historical)" should {
        "enable all versions the schema with the given name" in {
          val schA1 = SchemaFactory.getDummySchema(name = "schA", version = 1, disabled = true)
          val schA2 = SchemaFactory.getDummySchema(name = "schA", version = 2, disabled = false)
          schemaFixture.add(schA1, schA2)

          val response = sendPut[String, DisabledPayload](s"$apiUrl/schA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = false)

          // all versions enabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe false

          val responseA2 = sendGet[Schema](s"$apiUrl/schA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe false
        }
      }
    }

    "return 404" when {
      "no Schema with the given name exists" should {
        "enable nothing" in {
          val response = sendPut[String, DisabledPayload](s"$apiUrl/aSchema")
          assertNotFound(response)
        }
      }
    }

  }

  s"DELETE $apiUrl/{name}" can {
    "return 200" when {
      "a Schema with the given name exists" should {
        "disable the schema with the given name" in {
          val schA1 = SchemaFactory.getDummySchema(name = "schA", version = 1)
          val schA2 = SchemaFactory.getDummySchema(name = "schA", version = 2)
          val schB = SchemaFactory.getDummySchema(name = "schB", version = 1)
          schemaFixture.add(schA1, schA2, schB)

          val response = sendDelete[DisabledPayload](s"$apiUrl/schA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Schema](s"$apiUrl/schA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true

          // unrelated schema unaffected
          val responseB = sendGet[Schema](s"$apiUrl/schB/1")
          assertOk(responseB)
          responseB.getBody.disabled shouldBe false
        }
      }

      "a Schema with the given name exists and there have mixed (historical) disabled states " should {
        "disable all versions the schema with the given name" in {
          val schA1 = SchemaFactory.getDummySchema(name = "schA", version = 1, disabled = true)
          val schA2 = SchemaFactory.getDummySchema(name = "schA", version = 2, disabled = false)
          schemaFixture.add(schA1, schA2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/schA")
          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schA/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Schema](s"$apiUrl/schA/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
      "the Schema is only used in disabled Datasets" should {
        "disable the Schema" in {
          val dataset = DatasetFactory.getDummyDataset(schemaName = "schema", schemaVersion = 1, disabled = true)
          datasetFixture.add(dataset)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/schema")

          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schema/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Schema](s"$apiUrl/schema/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
      "the Schema is only used in disabled MappingTables" should {
        "disable the Schema" in {
          val mappingTable = MappingTableFactory.getDummyMappingTable(schemaName = "schema", schemaVersion = 1, disabled = true)
          mappingTableFixture.add(mappingTable)
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val response = sendDelete[DisabledPayload](s"$apiUrl/schema")

          assertOk(response)
          response.getBody shouldBe DisabledPayload(disabled = true)

          // all versions disabled
          val responseA1 = sendGet[Schema](s"$apiUrl/schema/1")
          assertOk(responseA1)
          responseA1.getBody.disabled shouldBe true

          val responseA2 = sendGet[Schema](s"$apiUrl/schema/2")
          assertOk(responseA2)
          responseA2.getBody.disabled shouldBe true
        }
      }
    }

    "return 400" when {
      "the Schema is used by an enabled Dataset" should {
        "return a list of the entities the Schema is used in" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", schemaName = "schema", schemaVersion = 1)
          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 7, schemaName = "schema", schemaVersion = 2)
          val dataset3 = DatasetFactory.getDummyDataset(name = "dataset3", schemaName = "anotherSchema", schemaVersion = 8) // moot
          val disabledDs = DatasetFactory.getDummyDataset(name = "disabledDs", schemaName = "schema", schemaVersion = 2, disabled = true)
          datasetFixture.add(dataset1, dataset2, dataset3, disabledDs)

          val response = sendDelete[EntityInUseException](s"$apiUrl/schema")

          assertBadRequest(response)
          response.getBody shouldBe EntityInUseException("""Cannot disable entity "schema", because it is used in the following entities""",
            UsedIn(Some(Seq(Reference(None, "dataset1", 1), Reference(None, "dataset2", 7))), None)
          )
        }
      }
      "the Schema is used by a enabled MappingTable" should {
        "return a list of the entities the Schema is used in" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mapping1", schemaName = "schema", schemaVersion = 1, disabled = false)
          val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mapping2", schemaName = "schema", schemaVersion = 2, disabled = false)
          mappingTableFixture.add(mappingTable1, mappingTable2)

          val response = sendDelete[EntityInUseException](s"$apiUrl/schema")
          assertBadRequest(response)

          response.getBody shouldBe EntityInUseException("""Cannot disable entity "schema", because it is used in the following entities""",
            UsedIn(None, Some(Seq(Reference(None, "mapping1", 1), Reference(None, "mapping2", 1))))
          )
        }
      }
      "the Schema is used by combination of MT and DS" should {
        "return a list of the entities the Schema is used in" in {
          val schema1 = SchemaFactory.getDummySchema(name = "schema", version = 1)
          val schema2 = SchemaFactory.getDummySchema(name = "schema", version = 2)
          schemaFixture.add(schema1, schema2)

          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mapping1", schemaName = "schema", schemaVersion = 1, disabled = false)
          mappingTableFixture.add(mappingTable1)

          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", schemaName = "schema", schemaVersion = 2)
          datasetFixture.add(dataset2)

          val response = sendDelete[EntityInUseException](s"$apiUrl/schema")
          assertBadRequest(response)

          response.getBody shouldBe EntityInUseException("""Cannot disable entity "schema", because it is used in the following entities""",
            UsedIn(Some(Seq(Reference(None, "dataset2", 1))), Some(Seq(Reference(None, "mapping1", 1))))
          )
        }
      }
    }

    "return 404" when {
      "no Schema with the given name exists" should {
        "disable nothing" in {
          val response = sendDelete[String](s"$apiUrl/aSchema")
          assertNotFound(response)
        }
      }
    }
  }
}
