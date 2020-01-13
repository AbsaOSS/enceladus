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
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.{MongoCollection, MongoDatabase}
import org.scalatest.Assertions

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class FixtureService[T](mongoDb: MongoDatabase, collectionName: String)(implicit ct: ClassTag[T])
  extends Assertions {

  protected val collection: MongoCollection[T] = mongoDb.getCollection[T](collectionName)

  def clearCollection(): Unit = {
    Await.ready(collection.deleteMany(new BsonDocument()).toFuture(), Duration.Inf)
  }

  def add(entities: T*): Unit = {
    try {
      Await.result(collection.insertMany(entities).toFuture(), Duration.Inf)
    } catch {
      case exception: MongoBulkWriteException =>
        fail(s"""${this.getClass.getSimpleName} failed to set up database:
                |    ${exception.getMessage}""".stripMargin)
    }
  }

}
