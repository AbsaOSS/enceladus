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

package za.co.absa.enceladus.migrations.repositories

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.{Completed, MongoDatabase}
import za.co.absa.enceladus.migrations.models.Evolution

import scala.concurrent.Future

class EvolutionMongoRepository(mongoDb: MongoDatabase) {

  private val codecRegistry = fromRegistries(fromProviders(classOf[Evolution]), DEFAULT_CODEC_REGISTRY )
  private val collection = mongoDb.getCollection[Evolution]("evolutions").withCodecRegistry(codecRegistry)

  def findAllOrdered(): Future[Seq[Evolution]] = collection.find().sort(ascending("order")).toFuture()

  def insert(evolution: Evolution): Future[Completed] = collection.insertOne(evolution).toFuture()

}
