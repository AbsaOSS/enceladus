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

package za.co.absa.enceladus.rest_api.integration.repositories

import org.junit.runner.RunWith
import org.scalatest.Assertion
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.rest_api.integration.fixtures.{AttachmentFixtureService, FixtureService}
import za.co.absa.enceladus.rest_api.repositories.{AttachmentMongoRepository, RefCollection}
import za.co.absa.enceladus.model.menas.MenasAttachment
import za.co.absa.enceladus.model.test.factories.AttachmentFactory

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class AttachmentRepositoryIntegrationSuite extends BaseRepositoryTest {

  @Autowired
  private val attachmentFixture: AttachmentFixtureService = null

  @Autowired
  private val attachmentRepository: AttachmentMongoRepository = null

  override def fixtures: List[FixtureService[_]] = List(attachmentFixture)

  private val schemaRefCollection = RefCollection.SCHEMA.name().toLowerCase()

  "AttachmentMongoRepository::getSchemaByNameAndVersion" should {
    "return None" when {
      "no Attachment exists for the specified name" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val actual = await(attachmentRepository.getSchemaByNameAndVersion("otherSchemaName", 2))

        assert(actual.isEmpty)
      }
      "no Attachment exists with a version up to the specified version" in {
        val attachment = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection)
        attachmentFixture.add(attachment)

        val actual = await(attachmentRepository.getSchemaByNameAndVersion("schemaName", 1))

        assert(actual.isEmpty)
      }
    }

    "return an Option of the nearest Attachment up to the specified version" when {
      "there are Attachments with previous and subsequent versions" in {
        val attachment1 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 1, refCollection = schemaRefCollection, fileContent = Array(1,2,3))
        val attachment2 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection, fileContent = Array(2,3,4))
        val attachment4 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 4, refCollection = schemaRefCollection, fileContent = Array(4,5,6))
        val attachment5 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 5, refCollection = schemaRefCollection, fileContent = Array(5,6,7))
        attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

        val actual = await(attachmentRepository.getSchemaByNameAndVersion("schemaName", 3))

        assertAttachment(actual, attachment2)
      }
      "there is an Attachment with the exact version" in {
        val attachment1 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 1, refCollection = schemaRefCollection, fileContent = Array(1,2,3))
        val attachment2 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 2, refCollection = schemaRefCollection, fileContent = Array(2,3,4))
        val attachment4 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 4, refCollection = schemaRefCollection, fileContent = Array(4,5,6))
        val attachment5 = AttachmentFactory.getDummyAttachment(refName = "schemaName", refVersion = 5, refCollection = schemaRefCollection, fileContent = Array(5,6,7))
        attachmentFixture.add(attachment1, attachment2, attachment4, attachment5)

        val actual = await(attachmentRepository.getSchemaByNameAndVersion("schemaName", 4))

        assertAttachment(actual, attachment4)
      }
    }
  }

  private def assertAttachment(actualOpt: Option[MenasAttachment], expectedAttachment: MenasAttachment): Assertion = {
    assert(actualOpt.isDefined)

    val actual = actualOpt.get
    assert(actual.fileContent.sameElements(expectedAttachment.fileContent))

    val expected = expectedAttachment.copy(fileContent = actual.fileContent)
    assert(actual == expected)
  }

}
