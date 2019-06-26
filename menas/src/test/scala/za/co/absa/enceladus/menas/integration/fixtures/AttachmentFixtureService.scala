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

package za.co.absa.enceladus.menas.integration.fixtures

import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.enceladus.menas.repositories.AttachmentMongoRepository
import za.co.absa.enceladus.model.menas.MenasAttachment

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@Component
class AttachmentFixtureService @Autowired()(mongoDb: MongoDatabase) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val collection = mongoDb.getCollection[MenasAttachment](AttachmentMongoRepository.collectionName)

  def createCollection(): Unit = {
    Await.ready(mongoDb.createCollection(AttachmentMongoRepository.collectionName).toFuture(), Duration.Inf)
  }

  def add(attachments: MenasAttachment*): Unit = {
    val futureAttachments = attachments.map { attachment =>
      collection.insertOne(attachment).head()
    }
    Await.ready(Future.sequence(futureAttachments), Duration.Inf)
  }

  def dropCollection(): Unit = {
    Await.ready(mongoDb.getCollection(AttachmentMongoRepository.collectionName).drop().toFuture(), Duration.Inf)
  }

}
