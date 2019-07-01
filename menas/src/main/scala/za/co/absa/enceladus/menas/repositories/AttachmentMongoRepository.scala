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

package za.co.absa.enceladus.menas.repositories

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model
import za.co.absa.enceladus.model.menas.MenasAttachment

import scala.concurrent.Future
import scala.reflect.ClassTag

object AttachmentMongoRepository {
  val collectionBaseName: String = "attachment"
  val collectionName: String = collectionBaseName + model.CollectionSuffix
}

@Repository
class AttachmentMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[MenasAttachment](mongoDb)(ClassTag(classOf[MenasAttachment])) {

  private[menas] override def collectionBaseName: String = AttachmentMongoRepository.collectionBaseName

  def getSchemaByNameAndVersion(name: String, version: Int): Future[Option[MenasAttachment]] = {
    getByCollectionAndNameAndVersion(RefCollection.SCHEMA.name().toLowerCase(), name, version)
  }

  private def getByCollectionAndNameAndVersion(refCollection: String,
                                               name: String,
                                               version: Int): Future[Option[MenasAttachment]] = {

    val pipeline = Seq(
      filter(equal("refCollection", refCollection)),
      filter(equal("refName", name)),
      project(Document(s"{diff: {$$subtract: [$version, '$$refVersion']}, doc: '$$$$ROOT'}")),
      filter(gte("diff", 0)),
      sort(Sorts.ascending("diff")),
      replaceRoot("$doc"),
      limit(1)
    )
    collection.aggregate(pipeline).headOption()
  }

}
