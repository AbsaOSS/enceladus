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

package za.co.absa.enceladus.migrations.continuous.migrate01

import org.mongodb.scala.MongoDatabase
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.ObjectIdTools
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.Serializer1
import za.co.absa.enceladus.migrations.migrations.{MigrationToV1, model1}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * The class provides continuous migration for schemas from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
final class SchemaMigrator(evm: EntityVersionMap,
                           databaseOld: MongoDatabase,
                           databaseNew: MongoDatabase) extends EntityMigrator(databaseOld, databaseNew) {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  override protected val collectionBase: String = EntityMigrator.schemaCollection

  /**
    * Migrates a single instance of schema.
    *
    * @param srcSchemaJson A JSON representation of a schema in the model 0 format.
    * @param objectId      An Object Id of the schema.
    * @param repo          An entity repository.
    */
  def migrateEntity(srcSchemaJson: String, objectId: String, repo: EntityRepository): Unit = {
    Try {
      val schema0 = Serializer0.deserializeSchema(srcSchemaJson)
      model1.Schema(
        schema0.name,
        schema0.version,
        None,
        userCreated = migrationUserName,
        userUpdated = migrationUserName,
        fields = schema0.fields.map(MigrationToV1.convertSchemaField(_, Nil))
      )
    } match {
      case Success(schema1) =>
        if (repo.doesDocumentExist(schema1.name, schema1.version)) {
          resolveConflict(schema1, objectId, repo)
        } else {
          normalInsert(schema1, objectId, repo)
        }
      case Failure(e) =>
        log.error(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
    }
  }

  /**
    * In case there is no conflict, insert a new schema normally.
    * If inserting is unsuccessful fallback to conflict resolution.
    *
    * @param schema   A schema to save as an instance of Model 1 object
    * @param objectId An Object Id of the schema
    * @param repo     An entity repository
    */
  def normalInsert(schema: model1.Schema, objectId: String, repo: EntityRepository): Unit = {
    val schema1Json = ObjectIdTools.putObjectIdIfNotPresent(Serializer1.serializeSchema(schema), Option(objectId))

    try {
      repo.insertDocument(schema1Json)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to append a document for 'schema': ${e.getMessage}. Retrying...")
        resolveConflict(schema, objectId, repo)
    }
  }

  /**
    * In case there is a conflict, resolve it by adding a new version of the schema
    * and register the mapping between the old version and the new one in the entity version map.
    *
    * @param schema   A schema to save as an instance of Model 1 object
    * @param objectId An Object Id if the schema
    * @param repo     An entity repository
    */
  def resolveConflict(schema: model1.Schema, objectId: String, repo: EntityRepository): Unit = {
    var retriesLeft = EntityMigrator.NumberOfInsertRetries
    var saved = false
    while (retriesLeft > 0 && !saved) {
      retriesLeft -= 1
      val latestVersion = repo.getLatestVersion(schema.name)
      val schemaToSave = schema.copy(version = latestVersion + 1)
      val schema1Json = ObjectIdTools.putObjectIdIfNotPresent(Serializer1.serializeSchema(schemaToSave),
        Option(objectId))

      try {
        repo.insertDocument(schema1Json)
        evm.add(collectionBase, schema.name, schema.version, schemaToSave.version)
        saved = true
      } catch {
        case NonFatal(e) =>
          if (retriesLeft > 0) {
            log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}. Retrying...")
          } else {
            throw e // Something went terribly wrong
          }
      }

    }
  }

}
