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

package za.co.absa.enceladus.rest.repositories

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.{Completed, MongoDatabase, Observable}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class MongoRepository[C](mongoDb: MongoDatabase)(implicit ct: ClassTag[C]) {
  private[repositories] val logger = LoggerFactory.getLogger(this.getClass)

  private[repositories] val collection = mongoDb.getCollection[C](collectionName)

  private[repositories] def collectionName: String

  private[repositories] val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  def isUniqueName(name: String): Observable[Boolean] = {
    val res = collection.countDocuments(getNameFilter(name))
    res.map( _ <= 0 )
  }

  def create(item: C): Future[Completed] = {
    collection.insertOne(item).head()
  }

  private[repositories] def getNameFilter(name: String): Bson = {
    equal("name", name)
  }

}
