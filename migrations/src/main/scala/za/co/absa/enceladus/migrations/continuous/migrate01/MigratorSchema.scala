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

import org.apache.log4j.{LogManager, Logger}
import org.mongodb.scala.MongoDatabase
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.{MigrationUtils, ObjectIdTools}
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.Serializer1
import za.co.absa.enceladus.migrations.migrations.{MigrationToV1, model1}

import scala.util.control.NonFatal

/**
  * The class provides schemas continuous migration from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
final class MigratorSchema(evm: EntityVersionMap,
                     databaseOld: MongoDatabase,
                     databaseNew: MongoDatabase) extends EntityMigrator {
  private val log: Logger = LogManager.getLogger(this.getClass)

  override protected val collectionBase: String = EntityMigrator.schemaCollection
  override protected val collectionOld: String = MigrationUtils.getVersionedCollectionName(collectionBase, 0)
  override protected val collectionNew: String = MigrationUtils.getVersionedCollectionName(collectionBase, 1)

  protected val dbOld: MongoDatabase = databaseOld
  protected val dbNew: MongoDatabase = databaseNew

  /**
    * Migrates a single instance of schema.
    *
    * @param srcSchemaJson A JSON representation of a schema in the model 0 format.
    * @param objectId      An Object Id if the schema.
    * @param repo          An entity repository.
    */
  def migrateEntity(srcSchemaJson: String, objectId: String, repo: EntityRepository): Unit = {
    val schema1Opt = try {
      val schema0 = Serializer0.deserializeSchema(srcSchemaJson)
      Option(model1.Schema(
        schema0.name,
        schema0.version,
        None,
        userCreated = migrationUserName,
        userUpdated = migrationUserName,
        fields = schema0.fields.map(MigrationToV1.convertSchemaField(_, Nil))
      ))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
        None
    }

    schema1Opt.foreach(schema1 => {
      if (repo.doesDocumentExist(schema1.name, schema1.version)) {
        resolveConflict(schema1, objectId, repo)
      } else {
        normalInsert(schema1, objectId, repo)
      }
    })
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
    * and register the mapping between the old version and the new one in ahe entity version map.
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
            log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}")
          } else {
            throw e // Something went terribly wrong
          }
      }

    }
  }

}
