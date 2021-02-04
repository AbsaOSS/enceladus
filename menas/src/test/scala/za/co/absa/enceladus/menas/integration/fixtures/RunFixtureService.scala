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

package za.co.absa.enceladus.menas.integration.fixtures

import com.mongodb.MongoBulkWriteException
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonDocument
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.atum.utils.SerializationUtils
import za.co.absa.enceladus.menas.repositories.RunMongoRepository
import za.co.absa.enceladus.model.Run

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@Component
class RunFixtureService @Autowired()(mongoDb: MongoDatabase)
  extends FixtureService[Run](mongoDb, RunMongoRepository.collectionName) {

  override def add(runs: Run*): Unit = {
    val bsonRuns = runs.map { run =>
      BsonDocument(SerializationUtils.asJson(run))
    }
    try {
      Await.result(collection.withDocumentClass[BsonDocument].insertMany(bsonRuns).toFuture(), Duration.Inf)
    } catch {
      case exception: MongoBulkWriteException =>
        fail(s"""${this.getClass.getSimpleName} failed to set up database:
                |    ${exception.getMessage}""".stripMargin)
    }
  }

}
