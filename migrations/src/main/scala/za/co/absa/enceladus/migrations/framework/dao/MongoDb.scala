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

package za.co.absa.enceladus.migrations.framework.dao

import org.apache.log4j.{LogManager, Logger}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.{MongoCollection, MongoDatabase, MongoNamespace}
import za.co.absa.enceladus.migrations.framework.model.DbVersion

import scala.reflect.ClassTag

class MongoDb (db: MongoDatabase) extends DocumentDb {
  private val log: Logger = LogManager.getLogger(this.getClass)

  val dbVersionCollectionName = "db_version"
  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DbVersion]), DEFAULT_CODEC_REGISTRY)

  // This adds .execute() method to observables
  import ScalaMongoImplicits._

  /**
    * Returns the version of the database. Version of a database determines the schema used for writes.
    *
    * For MongoDB databases the version number is kept in 'db_version' collection.
    * If there is no such a collection the version of the database is assumed to be 0.
    */
  override def getVersion(): Int = {
    if (!isCollectionExists(dbVersionCollectionName)) {
      setVersion(0)
    }

    val versions = getCollection(dbVersionCollectionName)
      .find[DbVersion]()
      .execute()

    if (versions.isEmpty) {
      getCollection[DbVersion](dbVersionCollectionName)
        .insertOne(DbVersion(0))
        .execute()
      0
    } else if (versions.lengthCompare(1) != 0) {
      val len = versions.length
      throw new IllegalStateException(
        s"Unexpected number of documents in '$dbVersionCollectionName'. Expected 1, got $len")
    } else {
      versions.head.version
    }
  }

  /**
    * Sets the version of the database. When a version is set it implies a migration to that version is completed
    * successfully and the database can be used with the specified version of the schema.
    *
    * For MongoDB databases the version number is kept in 'db_version' collection.
    * If there is no such a collection the version of the database is assumed to be 0.
    */
  override def setVersion(version: Int): Unit = {
    if (!isCollectionExists(dbVersionCollectionName)) {
      createCollection(dbVersionCollectionName)
      getCollection[DbVersion](dbVersionCollectionName)
        .insertOne(DbVersion(version))
        .execute()
    } else {
      getCollection[DbVersion](dbVersionCollectionName)
        .replaceOne(new BsonDocument, DbVersion(version))
        .execute()
    }
  }

  /**
    * Returns true if the specified collection exists in the database.
    */
  override def isCollectionExists(collectionName: String): Boolean = {
    val collections = db.listCollectionNames().execute()
    collections.contains(collectionName)
  }

  /**
    * Creates a collection with the given name.
    */
  override def createCollection(collectionName: String): Unit = {
    log.info(s"Creating $collectionName collection...")
    db.createCollection(collectionName).execute()
  }

  /**
    * Drop a collection.
    */
  override def dropCollection(collectionName: String): Unit = {
    log.info(s"Dropping $collectionName collection...")
    db.getCollection(collectionName)
      .drop()
      .execute()
  }

  /**
    * Removes all documents from a collection.
    */
  override def emptyCollection(collectionName: String): Unit = {
    log.info(s"Emptying $collectionName collection...")
    db.getCollection(collectionName)
      .deleteMany(new BsonDocument())
      .execute()
  }

  /**
    * Renames a collection
    */
  override def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit = {
    log.info(s"Renaming $collectionNameOld to $collectionNameNew...")
    db.getCollection(collectionNameOld)
      .renameCollection(MongoNamespace(db.name, collectionNameNew))
      .execute()
  }

  /**
    * Copies contents of a collection to a collection with a different name in the same database.
    */
  override def cloneCollection(collectionName: String, newCollectionName: String): Unit = {
    log.info(s"Copying $collectionName to $newCollectionName...")
    val cmd = s"""{
                |  aggregate: "$collectionName",
                |  pipeline: [
                |    {
                |      $$match: {}
                |    },
                |    {
                |      $$out: "$newCollectionName"
                |    }
                |  ],
                |  cursor: {}
                |}
                |""".stripMargin
    db.runCommand(BsonDocument(cmd)).execute()
  }

  /**
    * Returns the number of documents in the specified collection.
    */
  override def getDocumentsCount(collectionName: String): Long = {
    if (!isCollectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    db.getCollection(collectionName).countDocuments().execute()
  }

  /**
    * Inserts a document into a collection.
    */
  override def insertDocument(collectionName: String, document: String): Unit = {
    db.getCollection(collectionName)
      .insertOne(BsonDocument(document))
      .execute()
  }

  /**
    * Returns an iterator on all documents in the specified collection.
    */
  override def getDocuments(collectionName: String): Iterator[String] = {
    log.info(s"Getting all documents for $collectionName...")
    db.getCollection(collectionName)
      .find()
      .execute()
      .toIterator
      .map(_.toJson())
  }

  /**
    * Executes a command expressed in the database-specific language/format on the database.
    *
    * In MongoDB commands are expected to be in JSON format.
    *
    * Example:
    * {{{
    *   {
    *   aggregate: "foo",
    *   pipeline: [
    *     {
    *       $match: {}
    *     },
    *     {
    *       $out: "bar"
    *     }
    *   ],
    *   cursor: {},
    *   $readPreference: {
    *     mode: "secondaryPreferred"
    *   },
    *   $db: "blog"
    * }
    * }}}
    */
  override def executeCommand(cmd: String): Unit = {
    log.info(s"Executing MongoDB command: $cmd...")
    db.runCommand(BsonDocument(cmd)).execute()
  }

  /**
    * Returns a collection that is serialized as a case class.
    */
  private def getCollection[T](collectionName: String)(implicit ct: ClassTag[T]): MongoCollection[T] = {
    db.getCollection[T](dbVersionCollectionName)
      .withCodecRegistry(codecRegistry)
  }
}
