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
import org.mongodb.scala.bson.BsonDocument
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.menas.repositories.RunMongoRepository

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@Component
class RunFixtureService @Autowired()(mongoDb: MongoDatabase) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val collection = mongoDb.getCollection(RunMongoRepository.collectionName)

  def createCollection(): Unit = {
    Await.ready(mongoDb.createCollection(RunMongoRepository.collectionName).toFuture(), Duration.Inf)
  }

  def add(runs: Run*): Unit = {
    val futureRuns = runs.map { run =>
      val bson = BsonDocument(ControlUtils.asJson(run))
      collection.withDocumentClass[BsonDocument].insertOne(bson).head()
    }
    Await.ready(Future.sequence(futureRuns), Duration.Inf)
  }

  def dropCollection(): Unit = {
    Await.ready(mongoDb.getCollection(RunMongoRepository.collectionName).drop().toFuture(), Duration.Inf)
  }

}
