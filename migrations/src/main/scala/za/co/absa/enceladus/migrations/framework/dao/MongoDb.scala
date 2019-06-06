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
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{MongoCollection, MongoDatabase, MongoNamespace}
import za.co.absa.enceladus.migrations.framework.model.DbVersion
import za.co.absa.enceladus.migrations.framework.Constants.DatabaseVersionCollectionName

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * This is a MongoDB implementation of the document DB API needed for migration framework to run.
  *
  * The implementation depends on MongoDB Scala driver. An instance of Scala Mongo Database is expected to be
  * passed as a constructor parameter.
  */
class MongoDb(db: MongoDatabase) extends DocumentDb {
  private val log: Logger = LogManager.getLogger(this.getClass)

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
    if (!isCollectionExists(DatabaseVersionCollectionName)) {
      setVersion(0)
    }

    val versions = getCollection(DatabaseVersionCollectionName)
      .find[DbVersion]()
      .execute()

    if (versions.isEmpty) {
      getCollection[DbVersion](DatabaseVersionCollectionName)
        .insertOne(DbVersion(0))
        .execute()
      0
    } else if (versions.lengthCompare(1) != 0) {
      val len = versions.length
      throw new IllegalStateException(
        s"Unexpected number of documents in '$DatabaseVersionCollectionName'. Expected 1, got $len")
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
    if (!isCollectionExists(DatabaseVersionCollectionName)) {
      createCollection(DatabaseVersionCollectionName)
      getCollection[DbVersion](DatabaseVersionCollectionName)
        .insertOne(DbVersion(version))
        .execute()
    } else {
      getCollection[DbVersion](DatabaseVersionCollectionName)
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
    *
    * Copies indexed as well.
    */
  override def cloneCollection(collectionFrom: String, collectionTo: String): Unit = {
    log.info(s"Copying $collectionFrom to $collectionFrom...")
    val cmd =
      s"""{
         |  aggregate: "$collectionFrom",
         |  pipeline: [
         |    {
         |      $$match: {}
         |    },
         |    {
         |      $$out: "$collectionTo"
         |    }
         |  ],
         |  cursor: {}
         |}
         |""".stripMargin
    db.runCommand(BsonDocument(cmd)).execute()
    copyIndexes(collectionFrom, collectionTo)
  }

  /**
    * Creates an index for a given list of fields.
    */
  override def createIndex(collectionName: String, keys: Seq[String]): Unit = {
    log.info(s"Creating an index for $collectionName, keys: ${keys.mkString(",")}...")
    if (!isCollectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    try {
      db.getCollection(collectionName)
        .createIndex(fieldsToBsonKeys(keys))
        .execute()
    } catch {
      case NonFatal(e) => log.warn(s"Unable to create an index for $collectionName, keys: ${keys.mkString(",")}: "
        + e.getMessage)
    }
  }

  /**
    * Drops an index for a given list of fields.
    */
  override def dropIndex(collectionName: String, keys: Seq[String]): Unit = {
    log.info(s"Dropping an index for $collectionName, keys: ${keys.mkString(",")}...")
    if (!isCollectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    try {
      db.getCollection(collectionName)
        .dropIndex(fieldsToBsonKeys(keys))
        .execute()
    } catch {
      case NonFatal(e) => log.warn(s"Unable to drop an index for $collectionName, keys: ${keys.mkString(",")}: "
        + e.getMessage)
    }
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
    db.getCollection[T](DatabaseVersionCollectionName)
      .withCodecRegistry(codecRegistry)
  }

  /**
    * Returns a Bson document from the list of index key fields
    */
  private def fieldsToBsonKeys(keys: Seq[String]): Document = {
    val numbers = 1 to keys.size
    Document(keys.zip(numbers).map {
      case (field, num) => field -> num
    })
  }

  /**
    * Copies indexes from one collection to another. Skips the index on '_id' that is created automatically
    */
  private def copyIndexes(collectionFrom: String, collectionTo: String): Unit = {
    val indexes = db.getCollection(collectionFrom).listIndexes().execute()
    indexes.foreach(idxBson => {
      val indexDocument = idxBson("key").asDocument()
      if (!indexDocument.containsKey("_id")) {
        db.getCollection(collectionTo)
          .createIndex(indexDocument)
          .execute()
      }
    })
  }


}
