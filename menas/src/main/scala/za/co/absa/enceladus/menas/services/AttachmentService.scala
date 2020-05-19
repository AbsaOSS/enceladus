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

package za.co.absa.enceladus.menas.services

import org.mongodb.scala.Completed
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.menas.MenasAttachment
import za.co.absa.enceladus.menas.repositories._

import scala.concurrent.Future
import za.co.absa.enceladus.menas.exceptions.NotFoundException

@Service
class AttachmentService @Autowired()(attachmentMongoRepository: AttachmentMongoRepository,
                                     schemaMongoRepository: SchemaMongoRepository,
                                     datasetMongoRepository: DatasetMongoRepository,
                                     mappingTableMongoRepository: MappingTableMongoRepository)
  extends ModelService(attachmentMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  def uploadAttachment(attachment: MenasAttachment): Future[Completed] = {
    chooseRepository(attachment.refCollection).getLatestVersionValue(attachment.refName).flatMap {
      case Some(version) =>
        // the attachment version should already increased by one from the currently latest by the caller
        attachmentMongoRepository.create(attachment)
      case _ =>
        throw NotFoundException()
    }
  }

  def getSchemaByNameAndVersion(name: String, version: Int): Future[MenasAttachment] = {
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
