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
import org.springframework.http.{HttpStatus, MediaType}
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.test.factories.{AttachmentFactory, DatasetFactory, MappingTableFactory, SchemaFactory}
import za.co.absa.enceladus.model.{MappingTable, Schema, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.integration.controllers.{BaseRestApiTestV3, toExpected}
import za.co.absa.enceladus.rest_api.integration.fixtures._
import za.co.absa.enceladus.rest_api.models.SchemaApiFeatures
import za.co.absa.enceladus.rest_api.models.rest.RestResponse
import za.co.absa.enceladus.rest_api.models.rest.errors.{SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.rest_api.repositories.RefCollection
import za.co.absa.enceladus.rest_api.utils.SchemaType
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.restapi.TestResourcePath

import java.io.File
import java.nio.file.{Files, Path}
import scala.collection.immutable.HashMap

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class SchemaControllerV3IntegrationSuite extends BaseRestApiTestV3 with BeforeAndAfterAll with Matchers {

  private val port = 8877 // same  port as in test/resources/application.conf in the `menas.schemaRegistry.baseUrl` key
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

  @Autowired
  private val convertor: SparkMenasSchemaConvertor = null

  private val apiUrl = "/schemas"
  private val schemaRefCollection = RefCollection.SCHEMA.name().toLowerCase()

  override def fixtures: List[FixtureService[_]] = List(schemaFixture, attachmentFixture, datasetFixture, mappingTableFixture)

  s"POST $apiUrl" can {
    "return 201" when {
      "a Schema is created" in {
        val schema = SchemaFactory.getDummySchema("schemaA")

        val response = sendPostByAdmin[Schema, Validation](apiUrl, bodyOpt = Some(schema))

        assertCreated(response)
        val locationHeader = response.getHeaders.getFirst("location")
        locationHeader should endWith("/api-v3/schemas/schemaA/1")

        val response2 = sendGet[Schema]("/schemas/schemaA/1")
        assertOk(response2)

        val actual = response2.getBody
        val expected = toExpected(schema, actual)
        assert(actual == expected)
      }
    }

    "return 400" when {
      "an enabled Schema with that name already exists" in {
        val schema = SchemaFactory.getDummySchema()
        schemaFixture.add(schema)

        val response = sendPostByAdmin[Schema, Validation](apiUrl, bodyOpt = Some(schema))

        assertBadRequest(response)

        val actual = response.getBody
        val expected = Validation().withError("name", "entity with name already exists: 'dummyName'")
        assert(actual == expected)
      }
    }
  }

  // todo disable dataset - all versions/one version/ check the usage to prevent from disabling
  // todo used-in implementation checks

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
      "the schema has no fields" in { // todo 404 or 400 failed valiadation???
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
          val responseUploaded = sendPostUploadFileByAdmin[Validation](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Copybook.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("location")
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
          val responseUploaded = sendPostUploadFileByAdmin[Validation](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("location")
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

          val schemaParams = HashMap[String, Any](
            "name" -> schema.name, "version" -> schema.version, "format" -> "avro")
          val responseUploaded = sendPostUploadFileByAdmin[Schema](
            s"$apiUrl/schemaA/1/from-file", TestResourcePath.Avro.ok, schemaParams)
          assertCreated(responseUploaded)
          val locationHeader = responseUploaded.getHeaders.getFirst("location")
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
        val response = sendPostUploadFileByAdmin[String](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
        assertBadRequest(response)
        response.getBody should include("Required String parameter 'format' is not present")
      }

      "an empty upload format type is specified" in {
        val schema = SchemaFactory.getDummySchema("schemaA")
        schemaFixture.add(schema)

        val schemaParams = HashMap[String, Any]("format" -> "") // v2 fallbacked on this, v3 forbids it
        val response = sendPostUploadFileByAdmin[String](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Json.ok, schemaParams)
        assertBadRequest(response)
        response.getBody should include("not a recognized schema format. Menas currently supports: struct, copybook, avro.")
      }

      "a copybook with a syntax error" should {
        "return a response containing a schema parsing error with syntax error specific fields" in {
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "copybook")
          val response = sendPostUploadFileByAdmin[RestResponse](
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
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "struct")
          val response = sendPostUploadFileByAdmin[RestResponse](
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
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "avro")
          val response = sendPostUploadFileByAdmin[RestResponse](
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
          val schemaParams = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "foo")
          val response = sendPostUploadFileByAdmin[RestResponse](
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
    }

    "return 404" when {
      "a schema file is uploaded, but no schema exists for the specified name and version" in {
        val schemaParams = HashMap[String, Any](
          "name" -> "dummy", "version" -> 1, "format" -> "copybook")
        val responseUploaded = sendPostUploadFileByAdmin[Schema](
          s"$apiUrl/schemaA/1/from-file", TestResourcePath.Copybook.ok, schemaParams)
        assertNotFound(responseUploaded)
      }
    }
  }

  import com.github.tomakehurst.wiremock.client.WireMock._

  private def readTestResourceAsString(path: String): String = IOUtils.toString(getClass.getResourceAsStream(path))

  /**
   * will prepare the a response from file with correct `ContentType`
   */
  private def readTestResourceAsResponseWithContentType(path: String): ResponseDefinitionBuilder = {
    // this is crazy, but it works better than hardcoding mime-types
    val filePath: Path = new File(getClass.getResource(path).toURI()).toPath
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
          val responseRemoteLoaded = sendPostRemoteFileByAdmin[Schema](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("location")
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
          val responseRemoteLoaded = sendPostRemoteFileByAdmin[Schema](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("location")
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
          val responseRemoteLoaded = sendPostRemoteFileByAdmin[Schema](s"$apiUrl/schemaA/1/from-remote-uri", params)
          assertCreated(responseRemoteLoaded)
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("location")
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

            val params = HashMap("name" -> "MySchema", "version" -> 1, "format" -> schemaType.toString, "remoteUrl" -> remoteUrl)
            val response = sendPostRemoteFileByAdmin[RestResponse](s"$apiUrl/schemaA/1/from-remote-uri", params)
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

          val params = HashMap[String, Any]("version" -> 1, "name" -> "MySchema", "format" -> "foo", "remoteUrl" -> remoteUrl)
          val response = sendPostRemoteFileByAdmin[RestResponse](s"$apiUrl/schemaA/1/from-remote-uri", params)
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
          .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Copybook.ok)))

        val params = HashMap[String, Any]("version" -> 1, "name" -> "dummy", "format" -> "copybook", "remoteUrl" -> remoteUrl)
        val response = sendPostRemoteFileByAdmin[Schema](s"$apiUrl/schemaA/1/from-remote-uri", params)
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
          val responseRemoteLoaded = sendPostSubjectByAdmin[Schema](s"$apiUrl/schemaA/1/from-registry", params)
          assertCreated(responseRemoteLoaded)
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("location")
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
          val responseRemoteLoaded = sendPostSubjectByAdmin[Schema](s"$apiUrl/schemaA/1/from-registry", params)
          assertCreated(responseRemoteLoaded)
          val locationHeader = responseRemoteLoaded.getHeaders.getFirst("location")
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
  }

}
