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

import java.io.File
import java.nio.file.{Files, Path}

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.{HttpStatus, MediaType}
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.TestResourcePath
import za.co.absa.enceladus.menas.integration.fixtures._
import za.co.absa.enceladus.menas.models.rest.RestResponse
import za.co.absa.enceladus.menas.models.rest.errors.{SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.menas.models.{SchemaApiFeatures, Validation}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.test.factories.{AttachmentFactory, DatasetFactory, MappingTableFactory, PropertyDefinitionFactory, SchemaFactory}
import za.co.absa.enceladus.model.{Schema, UsedIn}

import scala.collection.immutable.HashMap

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class PropertyDefinitionApiIntegrationSuite extends BaseRestApiTest with BeforeAndAfterAll {

  @Autowired
  private val propertyDefinitionFixture: PropertyDefinitionFixtureService = null

  private val apiUrl = "/properties/datasets"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(propertyDefinitionFixture)

  s"POST $apiUrl/create" can {
    "return 201" when {
      "a PropertyDefinition is created" should {
        "return the created PropertyDefinition" in {
          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()

          val response = sendPost[PropertyDefinition, PropertyDefinition](s"$apiUrl/create", bodyOpt = Some(propertyDefinition))
          assertCreated(response)

          val actual = response.getBody
          val expected = toExpected(propertyDefinition, actual)
          assert(actual == expected)
        }
      }
      "all prior versions of the PropertyDefinition are disabled" should {
        "return the recreated PropertyDefinition" in {
          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
          propertyDefinitionFixture.add(propertyDefinition.copy(disabled = true))

          val response = sendPost[PropertyDefinition, PropertyDefinition](s"$apiUrl/create", bodyOpt = Some(propertyDefinition.setVersion(0)))
          assertCreated(response)

          val actual = response.getBody
          val expected = toExpected(propertyDefinition.setVersion(2), actual).copy(parent = Some(PropertyDefinitionFactory.toParent(propertyDefinition)))
          assert(actual == expected)
        }
      }
    }
    "return 400" when {
      "an enabled PropertyDefinition with that name already exists" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
        propertyDefinitionFixture.add(propertyDefinition)

        val response = sendPost[PropertyDefinition, Validation](s"$apiUrl/create", bodyOpt = Some(propertyDefinition))
        assertBadRequest(response)

        val actual = response.getBody
        val expected = Validation().withError("name", "entity with name already exists: 'dummyName'")
        assert(actual == expected)
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

            val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition")

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

            val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition")

            assertOk(response)

            val actual = response.getBody
            val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
            assert(actual == expected)
          }
        }
//        "any version of the PropertyDefinition is only used in disabled Datasets" should {
//          "disable all versions of the PropertyDefinition" in {
//            val dataset = DatasetFactory.getDummyDataset(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = true)
//            datasetFixture.add(dataset)
//            val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
//            val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
//            propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
//
//            val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition")
//
//            assertOk(response)
//
//            val actual = response.getBody
//            val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
//            assert(actual == expected)
//          }
//        }
//        "any version of the PropertyDefinition is only used in disabled MappingTables" should {
//          "disable all versions of the PropertyDefinition" in {
//            val mappingTable = MappingTableFactory.getDummyMappingTable(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = true)
//            mappingTableFixture.add(mappingTable)
//            val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
//            val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
//            propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
//
//            val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition")
//
//            assertOk(response)
//
//            val actual = response.getBody
//            val expected = """{"matchedCount":2,"modifiedCount":2,"upsertedId":null,"modifiedCountAvailable":true}"""
//            assert(actual == expected)
//          }
//        }
//        "no PropertyDefinition with the given name exists" should {
//          "disable nothing" in {
//            val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition")
//
//            assertOk(response)
//
//            val actual = response.getBody
//            val expected = """{"matchedCount":0,"modifiedCount":0,"upsertedId":null,"modifiedCountAvailable":true}"""
//            assert(actual == expected)
//          }
//        }
      }
  //
  //    "return 400" when {
  //      "some version of the PropertyDefinition is used by an enabled Dataset" should {
  //        "return a list of the entities the PropertyDefinition is used in" in {
  //          val dataset = DatasetFactory.getDummyDataset(name = "dataset", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          datasetFixture.add(dataset)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, UsedIn](s"$apiUrl/disable/propertyDefinition")
  //
  //          assertBadRequest(response)
  //
  //          val actual = response.getBody
  //          val expected = UsedIn(Some(Seq(MenasReference(None, "dataset", 1))), Some(Seq()))
  //          assert(actual == expected)
  //        }
  //      }
  //      "some version of the PropertyDefinition is used by a enabled MappingTable" should {
  //        "return a list of the entities the PropertyDefinition is used in" in {
  //          val mappingTable = MappingTableFactory.getDummyMappingTable(name = "mapping", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          mappingTableFixture.add(mappingTable)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, UsedIn](s"$apiUrl/disable/propertyDefinition")
  //
  //          assertBadRequest(response)
  //
  //          val actual = response.getBody
  //          val expected = UsedIn(Some(Seq()), Some(Seq(MenasReference(None, "mapping", 1))))
  //          assert(actual == expected)
  //        }
  //      }
  //    }
    }
  //
  //  s"DELETE $apiUrl/disable/{name}/{version}" can {
  //    "return 200" when {
  //      "a PropertyDefinition with the given name and version exists" should {
  //        "disable only the propertyDefinition with the given name and version" in {
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "otherPropertyDefinition", version = 1)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "multiple versions of the PropertyDefinition with the given name exist" should {
  //        "disable the specified version of the PropertyDefinition" in {
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "the version of the PropertyDefinition is only used in disabled Datasets" should {
  //        "disable the specified version of the PropertyDefinition" in {
  //          val dataset = DatasetFactory.getDummyDataset(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = true)
  //          datasetFixture.add(dataset)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "the version of the PropertyDefinition is not used in enabled Datasets" should {
  //        "disable the specified version of the PropertyDefinition" in {
  //          val dataset = DatasetFactory.getDummyDataset(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          datasetFixture.add(dataset)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/2")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "the version of the PropertyDefinition is only used in disabled MappingTables" should {
  //        "disable the specified version of the PropertyDefinition" in {
  //          val mappingTable = MappingTableFactory.getDummyMappingTable(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = true)
  //          mappingTableFixture.add(mappingTable)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "the version of the PropertyDefinition is not used in enabled MappingTables" should {
  //        "disable the specified version of the PropertyDefinition" in {
  //          val mappingTable = MappingTableFactory.getDummyMappingTable(propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          mappingTableFixture.add(mappingTable)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/2")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":1,"modifiedCount":1,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //      "no PropertyDefinition with the given name exists" should {
  //        "disable nothing" in {
  //          val response = sendDelete[PropertyDefinition, String](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertOk(response)
  //
  //          val actual = response.getBody
  //          val expected = """{"matchedCount":0,"modifiedCount":0,"upsertedId":null,"modifiedCountAvailable":true}"""
  //          assert(actual == expected)
  //        }
  //      }
  //    }
  //
  //    "return 400" when {
  //      "the version of the PropertyDefinition is used by an enabled Dataset" should {
  //        "return a list of the entities the version of the PropertyDefinition is used in" in {
  //          val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 2, disabled = false)
  //          datasetFixture.add(dataset1, dataset2)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, UsedIn](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertBadRequest(response)
  //
  //          val actual = response.getBody
  //          val expected = UsedIn(Some(Seq(MenasReference(None, "dataset1", 1))), Some(Seq()))
  //          assert(actual == expected)
  //        }
  //      }
  //      "some version of the PropertyDefinition is used by a enabled MappingTable" should {
  //        "return a list of the entities the version of the PropertyDefinition is used in" in {
  //          val mappingTable1 = MappingTableFactory.getDummyMappingTable(name = "mapping1", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 1, disabled = false)
  //          val mappingTable2 = MappingTableFactory.getDummyMappingTable(name = "mapping2", propertyDefinitionName = "propertyDefinition", propertyDefinitionVersion = 2, disabled = false)
  //          mappingTableFixture.add(mappingTable1, mappingTable2)
  //          val propertyDefinition1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 1)
  //          val propertyDefinition2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition", version = 2)
  //          propertyDefinitionFixture.add(propertyDefinition1, propertyDefinition2)
  //
  //          val response = sendDelete[PropertyDefinition, UsedIn](s"$apiUrl/disable/propertyDefinition/1")
  //
  //          assertBadRequest(response)
  //
  //          val actual = response.getBody
  //          val expected = UsedIn(Some(Seq()), Some(Seq(MenasReference(None, "mapping1", 1))))
  //          assert(actual == expected)
  //        }
  //      }
  //    }
  //  }

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


  //
  //  s"GET $apiUrl/export/{name}/{version}" should {
  //    "return 404" when {
  //      "no Attachment exists for the specified name" in {
  //        val attachment = AttachmentFactory.getDummyAttachment(refName = "propertyDefinitionName", refVersion = 2, refCollection = propertyDefinitionRefCollection)
  //        attachmentFixture.add(attachment)
  //
  //        val response = sendGet[Array[Byte]](s"$apiUrl/export/otherPropertyDefinitionName/2")
  //
  //        assertNotFound(response)
  //        assert(!response.getHeaders.containsKey("mime-type"))
  //      }
  //      "no Attachment exists with a version up to the specified version" in {
  //        val attachment = AttachmentFactory.getDummyAttachment(refName = "propertyDefinitionName", refVersion = 2, refCollection = propertyDefinitionRefCollection)
  //        attachmentFixture.add(attachment)
  //
  //        val response = sendGet[Array[Byte]](s"$apiUrl/export/propertyDefinitionName/1")
  //
  //        assertNotFound(response)
  //        assert(!response.getHeaders.containsKey("mime-type"))
  //      }
  //    }
  //
  //    "return 200" when {
  //      val attachment1 = AttachmentFactory.getDummyAttachment(
  //        refName = "propertyDefinitionName",
  //        refVersion = 1,
  //        refCollection = propertyDefinitionRefCollection,
  //        fileContent = Array(1, 2, 3))
  //      val attachment2 = AttachmentFactory.getDummyAttachment(
  //        refName = "propertyDefinitionName",
  //        refVersion = 2,
  //        refCollection = propertyDefinitionRefCollection,
  //        fileContent = Array(2, 3, 4),
  //        fileMIMEType = MediaType.APPLICATION_OCTET_STREAM_VALUE)
  //      val attachment4 = AttachmentFactory.getDummyAttachment(
  //        refName = "propertyDefinitionName",
  //        refVersion = 4,
  //        refCollection = propertyDefinitionRefCollection,
  //        fileContent = Array(4, 5, 6),
  //        fileMIMEType = MediaType.APPLICATION_JSON_VALUE)
  //      val attachment5 = AttachmentFactory.getDummyAttachment(
  //        refName = "propertyDefinitionName",
  //        refVersion = 5,
  //        refCollection = propertyDefinitionRefCollection,
  //        fileContent = Array(5, 6, 7))
  //      "there are Attachments with previous and subsequent versions" should {
  //        "return the byte array of the uploaded file with the nearest previous version" in {
  //          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)
  //
  //          val response = sendGet[Array[Byte]](s"$apiUrl/export/propertyDefinitionName/3")
  //
  //          assertOk(response)
  //          assert(response.getHeaders.containsKey("mime-type"))
  //          assert(response.getHeaders.get("mime-type").get(0) == MediaType.APPLICATION_OCTET_STREAM_VALUE)
  //
  //          val body = response.getBody
  //          assert(body.sameElements(attachment2.fileContent))
  //        }
  //      }
  //      "there is an Attachment with the exact version" should {
  //        "return the byte array of the uploaded file with the exact version" in {
  //          attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)
  //
  //          val response = sendGet[Array[Byte]](s"$apiUrl/export/propertyDefinitionName/4")
  //
  //          assertOk(response)
  //          assert(response.getHeaders.containsKey("mime-type"))
  //          assert(response.getHeaders.get("mime-type").get(0) == "application/json")
  //
  //          val body = response.getBody
  //          assert(body.sameElements(attachment4.fileContent))
  //        }
  //      }
  //    }
  //  }
  //
  //  s"POST $apiUrl/upload" should {
  //    "return 201" when {
  //      "a copybook has no errors" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          val propertyDefinitionParams = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "copybook")
  //          val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //            s"$apiUrl/upload", TestResourcePath.Copybook.ok, propertyDefinitionParams)
  //          assertCreated(responseUploaded)
  //
  //          val actual = responseUploaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 3)
  //        }
  //      }
  //
  //      "a JSON struct type propertyDefinition has no errors" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          val propertyDefinitionParams = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "struct")
  //          val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //            s"$apiUrl/upload", TestResourcePath.Json.ok, propertyDefinitionParams)
  //          assertCreated(responseUploaded)
  //
  //          val actual = responseUploaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 2)
  //        }
  //      }
  //
  //      "a JSON struct type propertyDefinition is uploaded, but an empty format type is specified" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          val propertyDefinitionParams = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "")
  //          val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //            s"$apiUrl/upload", TestResourcePath.Json.ok, propertyDefinitionParams)
  //          assertCreated(responseUploaded)
  //
  //          val actual = responseUploaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 2)
  //        }
  //      }
  //
  //      "a JSON struct type propertyDefinition is uploaded, but no format type is specified" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          val propertyDefinitionParams = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version)
  //          val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //            s"$apiUrl/upload", TestResourcePath.Json.ok, propertyDefinitionParams)
  //          assertCreated(responseUploaded)
  //
  //          val actual = responseUploaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 2)
  //        }
  //      }
  //
  //      "an avro propertyDefinition has no errors" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          val propertyDefinitionParams = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "avro")
  //          val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //            s"$apiUrl/upload", TestResourcePath.Avro.ok, propertyDefinitionParams)
  //          assertCreated(responseUploaded)
  //
  //          val actual = responseUploaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 7)
  //        }
  //      }
  //    }
  //
  //    "return 400" when {
  //      "a copybook with a syntax error" should {
  //        "return a response containing a propertyDefinition parsing error with syntax error specific fields" in {
  //          val propertyDefinitionParams = HashMap[String, Any]("version" -> 1, "name" -> "MyPropertyDefinition", "format" -> "copybook")
  //          val response = sendPostUploadFile[RestResponse](
  //            s"$apiUrl/upload", TestResourcePath.Copybook.bogus, propertyDefinitionParams)
  //          val body = response.getBody
  //
  //          assertBadRequest(response)
  //          body.error match {
  //            case Some(e: PropertyDefinitionParsingError) =>
  //              assert(e.errorType == "propertyDefinition_parsing")
  //              assert(e.propertyDefinitionType == PropertyDefinitionType.Copybook)
  //              assert(e.line.contains(22))
  //              assert(e.field.contains(""))
  //              assert(body.message.contains("Syntax error in the copybook"))
  //            case e => fail(s"Expected an instance of PropertyDefinitionParsingError, got $e.")
  //          }
  //        }
  //      }
  //
  //      "a JSON struct type propertyDefinition with a syntax error" should {
  //        "return a response containing a propertyDefinition parsing error returned by the StructType parser" in {
  //          val propertyDefinitionParams = HashMap[String, Any]("version" -> 1, "name" -> "MyPropertyDefinition", "format" -> "struct")
  //          val response = sendPostUploadFile[RestResponse](
  //            s"$apiUrl/upload", TestResourcePath.Json.bogus, propertyDefinitionParams)
  //          val body = response.getBody
  //
  //          assertBadRequest(response)
  //          body.error match {
  //            case Some(e: PropertyDefinitionParsingError) =>
  //              assert(e.errorType == "propertyDefinition_parsing")
  //              assert(e.propertyDefinitionType == PropertyDefinitionType.Struct)
  //              assert(body.message.contains("StructType serializer: Failed to convert the JSON string"))
  //            case e => fail(s"Expected an instance of PropertyDefinitionParsingError, got $e.")
  //          }
  //        }
  //      }
  //
  //      "an avro-propertyDefinition with a syntax error" should {
  //        "return a response containing a propertyDefinition parsing error encountered during avro propertyDefinition parsing" in {
  //          val propertyDefinitionParams = HashMap[String, Any]("version" -> 1, "name" -> "MyPropertyDefinition", "format" -> "avro")
  //          val response = sendPostUploadFile[RestResponse](
  //            s"$apiUrl/upload", TestResourcePath.Avro.bogus, propertyDefinitionParams)
  //          val body = response.getBody
  //
  //          assertBadRequest(response)
  //          body.error match {
  //            case Some(e: PropertyDefinitionParsingError) =>
  //              assert(e.errorType == "propertyDefinition_parsing")
  //              assert(e.propertyDefinitionType == PropertyDefinitionType.Avro)
  //              assert(body.message.contains("Record has no fields"))
  //            case e => fail(s"Expected an instance of PropertyDefinitionParsingError, got $e.")
  //          }
  //        }
  //      }
  //
  //      "a wrong format has been specified" should {
  //        "return a response containing a propertyDefinition format error" in {
  //          val propertyDefinitionParams = HashMap[String, Any]("version" -> 1, "name" -> "MyPropertyDefinition", "format" -> "foo")
  //          val response = sendPostUploadFile[RestResponse](
  //            s"$apiUrl/upload", TestResourcePath.Json.bogus, propertyDefinitionParams)
  //          val body = response.getBody
  //
  //          assertBadRequest(response)
  //          body.error match {
  //            case Some(e: PropertyDefinitionFormatError) =>
  //              assert(e.errorType == "propertyDefinition_format")
  //              assert(e.propertyDefinitionType == "foo")
  //              assert(body.message.contains("'foo' is not a recognized propertyDefinition format."))
  //            case e => fail(s"Expected an instance of PropertyDefinitionFormatError, got $e.")
  //          }
  //        }
  //      }
  //    }
  //
  //    "return 404" when {
  //      "a propertyDefinition file is uploaded, but no propertyDefinition exists for the specified name and version" in {
  //        val propertyDefinitionParams = HashMap[String, Any](
  //          "name" -> "dummy", "version" -> 1, "format" -> "copybook")
  //        val responseUploaded = sendPostUploadFile[PropertyDefinition](
  //          s"$apiUrl/upload", TestResourcePath.Copybook.ok, propertyDefinitionParams)
  //        assertNotFound(responseUploaded)
  //      }
  //    }
  //  }


  // PD specific:
  s"GET $apiUrl/{name}/{version}" should {
    "return 404" when {
      "no propertyDefinition exists for the specified name" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
        propertyDefinitionFixture.add(propertyDefinition)

        val response = sendGet[String](s"$apiUrl/json/otherPropertyDefinitionName/1")
        assertNotFound(response)
      }

      "no propertyDefinition exists for the specified version" in {
        val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
        propertyDefinitionFixture.add(propertyDefinition)

        val response = sendGet[String](s"$apiUrl/propertyDefinition1/123")
        assertNotFound(response)
      }
    }
    "return 200" when {
      "there is a PropertyDefinition with the specified name and version" should {
        "return the PropertyDefinition as a JSON" in {
          val pd1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
          propertyDefinitionFixture.add(pd1)

          val response = sendGet[String](s"$apiUrl/propertyDefinition1/1")
          assertOk(response)

          val body = response.getBody
          val expected =
            s"""{"name":"propertyDefinition1","version":1,"description":null,"propertyType":{"_t":"StringPropertyType"},
               |"suggestedValue":"","putIntoInfoFile":false,"essentiality":{"_t":"Optional"},"disabled":false,
               |"dateCreated":"${pd1.dateCreated}","userCreated":"dummyUser","lastUpdated":"${pd1.lastUpdated}",
               |"userUpdated":"dummyUser","dateDisabled":null,"userDisabled":null,"parent":null,"isRequired":false,
               |"isOptional":true,"createdMessage":{"menasRef":{"collection":null,"name":"propertyDefinition1","version":1},
               |"updatedBy":"dummyUser","updated":"${pd1.createdMessage.updated}","changes":[{"field":"","oldValue":null,"newValue":null,
               |"message":"PropertyDefinition propertyDefinition1 created."}]}}""".stripMargin.replaceAll("[\\r\\n]", "")
          assert(body == expected)
        }
      }
    }
  }

  s"GET $apiUrl/{name}" should {
    "return 200" when {
      "there is a PropertyDefinition with the latest name and version (regardless of being disabled)" should {
        "return the PropertyDefinition as a JSON" in {
          val pd1 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 1)
          val pd2 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 2)
          val pd3 = PropertyDefinitionFactory.getDummyPropertyDefinition(name = "propertyDefinition1", version = 3, disabled = true)
          propertyDefinitionFixture.add(pd1, pd2, pd3)

          val response = sendGet[PropertyDefinition](s"$apiUrl/propertyDefinition1")
          assertOk(response)

          val bodyVersion = response.getBody.version
          assert(bodyVersion == 3)
        }
      }
    }
  }


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

  //
  //  s"POST $apiUrl/remote" should {
  //
  //    val remoteFilePath = "/remote-test/someRemoteFile.ext"
  //    val remoteUrl = s"http://localhost:$port$remoteFilePath"
  //
  //    "return 201" when {
  //      "a copybook has no errors" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Copybook.ok)))
  //
  //          val params = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "copybook", "remoteUrl" -> remoteUrl)
  //          val responseRemoteLoaded = sendPostRemoteFile[PropertyDefinition](s"$apiUrl/remote", params)
  //          assertCreated(responseRemoteLoaded)
  //
  //          val actual = responseRemoteLoaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 3)
  //        }
  //      }
  //
  //      Seq(
  //        ("explicit JSON struct format", Some("format" -> "struct")),
  //        ("implicit JSON struct (by empty string format)", Some("format" -> "")),
  //        ("implicit JSON struct (by no format at all)", None)
  //      ).foreach { case (name, formatSpec) =>
  //        s"an $name propertyDefinition has no errors" should {
  //          "return a new version of the propertyDefinition" in {
  //            val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //            propertyDefinitionFixture.add(propertyDefinition)
  //
  //            wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //              .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Json.ok)))
  //
  //            // conditionally adding ("format" -> "struct"/"") or no format at all
  //            val params = HashMap("name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "remoteUrl" -> remoteUrl) ++ formatSpec
  //            val responseRemoteLoaded = sendPostRemoteFile[PropertyDefinition](s"$apiUrl/remote", params)
  //            assertCreated(responseRemoteLoaded)
  //
  //            val actual = responseRemoteLoaded.getBody
  //            assert(actual.name == propertyDefinition.name)
  //            assert(actual.version == propertyDefinition.version + 1)
  //            assert(actual.fields.length == 2)
  //          }
  //        }
  //      }
  //
  //      "an avro propertyDefinition has no errors" should {
  //        "return a new version of the propertyDefinition" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))
  //
  //          val params = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "avro", "remoteUrl" -> remoteUrl)
  //          val responseRemoteLoaded = sendPostRemoteFile[PropertyDefinition](s"$apiUrl/remote", params)
  //          assertCreated(responseRemoteLoaded)
  //
  //          val actual = responseRemoteLoaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 7)
  //        }
  //      }
  //    }
  //
  //    "return 400" when {
  //      Seq(
  //        (PropertyDefinitionType.Copybook, TestResourcePath.Copybook.bogus, "Syntax error in the copybook"),
  //        (PropertyDefinitionType.Struct, TestResourcePath.Json.bogus, "StructType serializer: Failed to convert the JSON string"),
  //        (PropertyDefinitionType.Avro, TestResourcePath.Avro.bogus, "Record has no fields")
  //      ).foreach { case (propertyDefinitionType, testResourcePath, expectedErrorMessage) =>
  //
  //        s"a $propertyDefinitionType with a syntax error" should {
  //          "return a response containing a propertyDefinition parsing error with syntax error specific fields" in {
  //            wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //              .willReturn(readTestResourceAsResponseWithContentType(testResourcePath)))
  //
  //            val params = HashMap("name" -> "MyPropertyDefinition", "version" -> 1, "format" -> propertyDefinitionType.toString, "remoteUrl" -> remoteUrl)
  //            val response = sendPostRemoteFile[RestResponse](s"$apiUrl/remote", params)
  //            val body = response.getBody
  //
  //            assertBadRequest(response)
  //            body.error match {
  //              case Some(e: PropertyDefinitionParsingError) =>
  //                assert(e.errorType == "propertyDefinition_parsing")
  //                assert(e.propertyDefinitionType == propertyDefinitionType)
  //                assert(body.message.contains(expectedErrorMessage))
  //              case e => fail(s"Expected an instance of PropertyDefinitionParsingError, got $e.")
  //            }
  //          }
  //        }
  //      }
  //
  //      "a wrong format has been specified" should {
  //        "return a response containing a propertyDefinition format error" in {
  //          wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Json.ok)))
  //
  //          val params = HashMap[String, Any]("version" -> 1, "name" -> "MyPropertyDefinition", "format" -> "foo", "remoteUrl" -> remoteUrl)
  //          val response = sendPostRemoteFile[RestResponse](s"$apiUrl/remote", params)
  //          val body = response.getBody
  //
  //          assertBadRequest(response)
  //          body.error match {
  //            case Some(e: PropertyDefinitionFormatError) =>
  //              assert(e.errorType == "propertyDefinition_format")
  //              assert(e.propertyDefinitionType == "foo")
  //              assert(body.message.contains("'foo' is not a recognized propertyDefinition format."))
  //            case e => fail(s"Expected an instance of PropertyDefinitionFormatError, got $e.")
  //          }
  //        }
  //      }
  //    }
  //
  //    "return 404" when {
  //      "a propertyDefinition file is loaded from remote url, but no propertyDefinition exists for the specified name and version" in {
  //        wireMockServer.stubFor(get(urlPathEqualTo(remoteFilePath))
  //          .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Copybook.ok)))
  //
  //        val params = HashMap[String, Any]("version" -> 1, "name" -> "dummy", "format" -> "copybook", "remoteUrl" -> remoteUrl)
  //        val response = sendPostRemoteFile[PropertyDefinition](s"$apiUrl/remote", params)
  //        assertNotFound(response)
  //      }
  //    }
  //  }
  //
  //  s"POST $apiUrl/registry" should {
  //    def subjectPath(subjectName: String) = s"/subjects/$subjectName/versions/latest/propertyDefinition"
  //
  //    "return 201" when {
  //      "an avro propertyDefinition has no errors" should {
  //        "load propertyDefinition by subject name as-is" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic1-value")))
  //            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))
  //
  //          val params = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "avro", "subject" -> "myTopic1-value")
  //          val responseRemoteLoaded = sendPostSubject[PropertyDefinition](s"$apiUrl/registry", params)
  //          assertCreated(responseRemoteLoaded)
  //
  //          val actual = responseRemoteLoaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 7)
  //        }
  //
  //        "load propertyDefinition by subject name -value fallback" in {
  //          val propertyDefinition = PropertyDefinitionFactory.getDummyPropertyDefinition()
  //          propertyDefinitionFixture.add(propertyDefinition)
  //
  //          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic2"))) // will fail
  //            .willReturn(notFound()))
  //
  //          wireMockServer.stubFor(get(urlPathEqualTo(subjectPath("myTopic2-value"))) // fallback will kick in
  //            .willReturn(readTestResourceAsResponseWithContentType(TestResourcePath.Avro.ok)))
  //
  //          val params = HashMap[String, Any](
  //            "name" -> propertyDefinition.name, "version" -> propertyDefinition.version, "format" -> "avro", "subject" -> "myTopic2")
  //          val responseRemoteLoaded = sendPostSubject[PropertyDefinition](s"$apiUrl/registry", params)
  //          assertCreated(responseRemoteLoaded)
  //
  //          val actual = responseRemoteLoaded.getBody
  //          assert(actual.name == propertyDefinition.name)
  //          assert(actual.version == propertyDefinition.version + 1)
  //          assert(actual.fields.length == 7)
  //        }
  //      }
  //    }
  //
  //    s"GET $apiUrl/features" can {
  //      "show propertyDefinition registry availability" when {
  //        "propertyDefinition registry integration is enabled" in {
  //
  //          val response = sendGet[PropertyDefinitionApiFeatures](s"$apiUrl/features")
  //          assert(response.getStatusCode == HttpStatus.OK)
  //          val responseBody = response.getBody
  //
  //          // test-config contains populated menas.propertyDefinitionRegistry.baseUrl
  //          assert(responseBody == PropertyDefinitionApiFeatures(registry = true))
  //        }
  //      }
  //    }
  //  }

  private def toExpected(propertyDefinition: PropertyDefinition, actual: PropertyDefinition): PropertyDefinition = {
    propertyDefinition.copy(
      dateCreated = actual.dateCreated,
      userCreated = actual.userCreated,
      lastUpdated = actual.lastUpdated,
      userUpdated = actual.userUpdated,
      dateDisabled = actual.dateDisabled,
      userDisabled = actual.userDisabled)
  }
}
