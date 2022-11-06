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

package za.co.absa.enceladus.rest_api.services

import org.mongodb.scala.Completed
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.backend.Attachment
import za.co.absa.enceladus.rest_api.repositories._

import scala.concurrent.Future
import za.co.absa.enceladus.rest_api.exceptions.NotFoundException

@Service
class AttachmentService @Autowired()(val mongoRepository: AttachmentMongoRepository,
                                     schemaMongoRepository: SchemaMongoRepository,
                                     datasetMongoRepository: DatasetMongoRepository,
                                     mappingTableMongoRepository: MappingTableMongoRepository)
  extends ModelService[Attachment] {

  protected val attachmentMongoRepository: AttachmentMongoRepository = mongoRepository // alias

  import scala.concurrent.ExecutionContext.Implicits.global

  def uploadAttachment(attachment: Attachment): Future[Completed] = {
    chooseRepository(attachment.refCollection).getLatestVersionValue(attachment.refName).flatMap {
      case Some(_) =>
        // the attachment version should already be increased by one from the currently latest by the caller
        attachmentMongoRepository.create(attachment)
      case _ =>
        throw NotFoundException()
    }
  }

  def getSchemaByNameAndVersion(name: String, version: Int): Future[Attachment] = {
    attachmentMongoRepository.getSchemaByNameAndVersion(name, version).map {
      case Some(attachment) => attachment
      case None             => throw NotFoundException()
    }
  }

  private def chooseRepository(refCollection: String): VersionedMongoRepository[_] = {
    RefCollection.byValueIgnoreCase(refCollection) match {
      case RefCollection.SCHEMA        => schemaMongoRepository
      case RefCollection.DATASET       => datasetMongoRepository
      case RefCollection.MAPPING_TABLE => mappingTableMongoRepository
    }
  }

}
