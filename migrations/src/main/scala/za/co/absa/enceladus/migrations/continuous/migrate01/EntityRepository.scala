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

package za.co.absa.enceladus.migrations.continuous.migrate01

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.{BsonDocument, ObjectId}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.{and, equal, or}
import org.mongodb.scala.model.Sorts._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.model.RunState
import za.co.absa.enceladus.migrations.framework.ObjectIdTools

/**
  * The class contains a set of MongoDB versioned collection manipulation routines needed for continuous migration.
  */
final class EntityRepository(db: MongoDatabase, collectionName: String) {

  import za.co.absa.enceladus.migrations.framework.dao.ScalaMongoImplicits._

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Returns iterator to a sorted collection of documents. Documents are sorted by name and version.
    *
    * @return An iterator to a JSON representation of the documents.
    */
  def getSortedDocuments: Iterator[String] = {
    db.getCollection(collectionName)
      .find()
      .sort(ascending("name", "version"))
      .execute()
      .toIterator
      .map(_.toJson)
  }

  /**
    * Transforms all runs in a sorted order.
    *
    * @param f A transformer function for a run.
    * @return An iterator to a JSON representation of the documents.
    */
  def processSortedRuns(f: String => Unit): Unit = {
    db.getCollection(collectionName)
      .find(
        or(
          equal("runStatus.status", RunState.allSucceeded.toString),
          equal("runStatus.status", RunState.failed.toString)
        )
      )
      .sort(ascending("dataset", "datasetVersion", "runId"))
      .foreach(s => f(s.toJson))
  }

  /**
    * Returns true if a document with the particular Object Id exists in the collection.
    *
    * @param objectId An object Id of an entity.
    * @return true if such a document exists, false otherwise.
    */
  def doesDocumentExist(objectId: String): Boolean = {
    val id = ObjectIdTools.extractId(objectId)
    val documents = db.getCollection(collectionName)
      .find(Filters.equal("_id", new ObjectId(id)))
      .execute()
    documents.nonEmpty
  }

  /**
    * Returns true if a document with the particular name and version exists in the collection.
    *
    * @param name    A name of an entity.
    * @param version A version of an entity.
    * @return true if such a document exists, false otherwise.
    */
  def doesDocumentExist(name: String, version: Int): Boolean = {
    val documents = db.getCollection(collectionName)
      .find(
        and(
          equal("name", name),
          equal("version", version))
      )
      .execute()
    documents.nonEmpty
  }

  /**
    * Returns true if a run with a particular runId, datasetName and datasetVersion exists in the collection.
    *
    * @param datasetName    A name of an entity.
    * @param datasetVersion A version of an entity.
    * @return true if such a document exists, false otherwise.
    */
  def doesRunExist(runId: Int, datasetName: String, datasetVersion: Int): Boolean = {
    val documents = db.getCollection(collectionName)
      .find(
        and(
          equal("runId", runId),
          equal("dataset", datasetName),
          equal("datasetVersion", datasetVersion)
        )
      )
      .execute()
    documents.nonEmpty
  }

  /**
    * Gets the latest version of a document having a particular name.
    *
    * @param name A name of an entity.
    * @return the latest version of an entity, or 0 if the collection is empty.
    */
  def getLatestVersion(name: String): Int = {
    val documents = db.getCollection(collectionName)
      .find(equal("name", name))
      .sort(descending("version"))
      .limit(1)
      .execute()
    documents.headOption.fold(0)(_.getInteger("version").toInt)
  }

  /**
    * Gets the latest version of a run having a particular dataset name and version.
    *
    * @param datasetName    A name of a dataset.
    * @param datasetVersion A version of a dataset.
    * @return the latest run id of an entity, or 0 if the collection is empty.
    */
  def getLatestRunId(datasetName: String, datasetVersion: Int): Int = {
    val documents = db.getCollection(collectionName)
      .find(
        and(
          equal("dataset", datasetName),
          equal("datasetVersion", datasetVersion))
      )
      .sort(descending("runId"))
      .limit(1)
      .execute()
    documents.headOption.map(_.getInteger("runId").toInt).getOrElse(0)
  }

  /**
    * Inserts a document into a collection.
    *
    * @param document A JSON representation of a document.
    */
  def insertDocument(document: String): Unit = {
    log.debug(s"INSERT INTO $collectionName: $document")
    db.getCollection(collectionName)
      .insertOne(BsonDocument(document))
      .execute()
  }
}
