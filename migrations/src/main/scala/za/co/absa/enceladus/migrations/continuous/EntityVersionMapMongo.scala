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

package za.co.absa.enceladus.migrations.continuous

import org.apache.log4j.{LogManager, Logger}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{MongoCollection, MongoDatabase}
import za.co.absa.enceladus.migrations.continuous.model.EntityVersionMapping
import za.co.absa.enceladus.migrations.framework.dao.ScalaMongoImplicits

import scala.reflect.ClassTag

class EntityVersionMapMongo(db: MongoDatabase) extends EntityVersionMap {
  private val log: Logger = LogManager.getLogger(this.getClass)

  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[EntityVersionMapping]),
    DEFAULT_CODEC_REGISTRY)

  // This adds .execute() method to observables
  import ScalaMongoImplicits._

  private val entityMapCollectionName = "entity_version_map"

  ensureCollectionExists()

  /**
    * An implementation if this abstract class should redefine this method. Clients of this class should use add()
    * as it does additional checks.
    *
    * @param collectionName The name of the collection that contains the entity
    * @param entityName     An entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @param newVersion     An version of the entity in the new version of the database
    */
  override protected def addEntry(collectionName: String,
                                  entityName: String,
                                  oldVersion: Int,
                                  newVersion: Int): Unit = {
    getTypedCollection[EntityVersionMapping](entityMapCollectionName)
      .insertOne(EntityVersionMapping(collectionName, entityName, oldVersion, newVersion))
      .execute()
  }

  /**
    * Gets a 'name - version' mapping.
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @return An version of the entity in the new version of the database, None if the entity is not found
    *         in the mapping
    */
  @throws[IllegalStateException]
  override def get(collectionName: String,
                   entityName: String,
                   oldVersion: Int): Option[Int] = {
    val mappings = getTypedCollection[EntityVersionMapping](entityMapCollectionName)
      .find(getFilter(collectionName, entityName, oldVersion))
      .execute()
    if (mappings.lengthCompare(1) > 0) {
      throw new IllegalStateException("Duplicate entries in entity version mappings found. " +
        s"Values: $mappings.")
    } else {
      mappings.headOption.map(record => record.newVersion)
    }
  }

  /**
    * Returns a collection that is serialized as a case class.
    */
  private def getTypedCollection[T](collectionName: String)(implicit ct: ClassTag[T]): MongoCollection[T] = {
    db.getCollection[T](entityMapCollectionName)
      .withCodecRegistry(codecRegistry)
  }

  /**
    * Returns a filter for the MongoDB query for finding entity version mapping.
    */
  private def getFilter(collectionName: String,
                        entityName: String,
                        oldVersion: Int): Bson = {
    and(
      and(
        regex("collection", collectionName, "i"),
        regex("entityName", entityName, "i")),
      equal("oldVersion", oldVersion))
  }

  /**
    * Ensures that the entity version mapping collections exists
    */
  private def ensureCollectionExists(): Unit = {
    val collections = db.listCollectionNames().execute()
    if (!collections.contains(entityMapCollectionName)) {
      db.createCollection(entityMapCollectionName).execute()
    }
  }

}
