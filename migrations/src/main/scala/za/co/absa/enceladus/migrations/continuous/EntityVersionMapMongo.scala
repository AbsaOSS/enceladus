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

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.IndexOptions
import org.mongodb.scala.{MongoCollection, MongoDatabase}
import za.co.absa.enceladus.migrations.continuous.model.EntityVersionMapping
import za.co.absa.enceladus.migrations.framework.dao.ScalaMongoImplicits

import scala.reflect.ClassTag

final class EntityVersionMapMongo(db: MongoDatabase) extends EntityVersionMap {
  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[EntityVersionMapping]),
    DEFAULT_CODEC_REGISTRY)

  import ScalaMongoImplicits._

  private val entityMapCollectionName = "entity_version_map"

  ensureCollectionExists()

  /**
    * A MongoDB implementation of an entity name-version mapping persistence.
    *
    * @param collectionName A collection name corresponding to an entity type
    * @param entityName     An entity name
    * @param oldVersion     A version of an entity in the old version of the database
    * @param newVersion     A version of an entity in the new version of the database
    */
  override protected def addEntry(collectionName: String,
                                  entityName: String,
                                  oldVersion: Int,
                                  newVersion: Int): Unit = {
    getTypedCollection[EntityVersionMapping]
      .insertOne(EntityVersionMapping(collectionName, entityName, oldVersion, newVersion))
      .execute()
  }

  /**
    * Gets a 'name - version' mapping.
    *
    * @param collectionName A collection name corresponding to an entity type
    * @param entityName     An entity name
    * @param oldVersion     A version of an entity in the old version of the database
    * @return An option of the version of the entity in the new version of the database, None if the entity is not found
    *         in the mapping
    */
  @throws[IllegalStateException]
  override def get(collectionName: String,
                   entityName: String,
                   oldVersion: Int): Option[Int] = {
    val mappings = getTypedCollection[EntityVersionMapping]
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
  private def getTypedCollection[T](implicit ct: ClassTag[T]): MongoCollection[T] = {
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
      equal("collection", collectionName),
      equal("entityName", entityName),
      equal("oldVersion", oldVersion))
  }

  /**
    * Ensures that the entity version mapping collections exists
    */
  private def ensureCollectionExists(): Unit = {
    val collections = db.listCollectionNames().execute()
    if (!collections.contains(entityMapCollectionName)) {
      db.createCollection(entityMapCollectionName).execute()
      db.getCollection(entityMapCollectionName)
        .createIndex(
          Document(
            Seq("collection" -> 1, "entityName" -> 1, "oldVersion" -> 1)
          ),
          IndexOptions().unique(true))
        .execute()
    }
  }

}
