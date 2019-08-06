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

import java.time.ZonedDateTime

import org.apache.log4j.{LogManager, Logger}
import org.mongodb.scala.MongoDatabase
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.{MigrationUtils, ObjectIdTools}
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1
import za.co.absa.enceladus.migrations.migrations.model1.{DefaultValue, Serializer1}

import scala.util.control.NonFatal

/**
  * The class provides mapping tables continuous migration from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
final class MigratorMappingTable(evm: EntityVersionMap,
                           databaseOld: MongoDatabase,
                           databaseNew: MongoDatabase) extends EntityMigrator {
  private val log: Logger = LogManager.getLogger(this.getClass)

  override protected val collectionBase: String = EntityMigrator.mappingTableCollection
  override protected val collectionOld: String = MigrationUtils.getVersionedCollectionName(collectionBase, 0)
  override protected val collectionNew: String = MigrationUtils.getVersionedCollectionName(collectionBase, 1)

  protected val dbOld: MongoDatabase = databaseOld
  protected val dbNew: MongoDatabase = databaseNew

  /**
    * Migrates a single instance of mapping table.
    *
    * @param srcMappingTableJson A JSON representation of a mapping table in the model 0 format.
    * @param objectId            An Object Id of the mapping table.
    * @param repo                An entity repository.
    */
  def migrateEntity(srcMappingTableJson: String, objectId: String, repo: EntityRepository): Unit = {
    val mappingTable1Opt = try {
      val mappingTable0 = Serializer0.deserializeMappingTable(srcMappingTableJson)

      val defaultValueList = mappingTable0.defaultMappingValue match {
        case Some(value) => List(DefaultValue("*", value))
        case None => Nil
      }

      Option(model1.MappingTable(
        mappingTable0.name,
        mappingTable0.version,
        None,
        mappingTable0.hdfsPath,
        mappingTable0.schemaName,
        evm.getSafeVersion(EntityMigrator.schemaCollection, mappingTable0.schemaName, mappingTable0.schemaVersion),
        defaultValueList,
        ZonedDateTime.now(),
        migrationUserName,
        ZonedDateTime.now(),
        migrationUserName
      ))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
        None
    }

    mappingTable1Opt.foreach(mappingTable1 => {
      if (repo.doesDocumentExist(mappingTable1.name, mappingTable1.version)) {
        resolveConflict(mappingTable1, objectId, repo)
      } else {
        normalInsert(mappingTable1, objectId, repo)
      }
    })
  }

  /**
    * In case there is no conflict, insert a new mapping table normally.
    * If inserting is unsuccessful fallback to conflict resolution.
    *
    * @param mappingTable A mapping table to save as an instance of Model 1 object
    * @param objectId     An Object Id if the mapping table
    * @param repo         An entity repository
    */
  def normalInsert(mappingTable: model1.MappingTable, objectId: String, repo: EntityRepository): Unit = {
    val mappingTable1Json = ObjectIdTools.putObjectIdIfNotPresent(Serializer1.serializeMappingTable(mappingTable),
      Option(objectId))

    try {
      repo.insertDocument(mappingTable1Json)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}. Retrying...")
        resolveConflict(mappingTable, objectId, repo)
    }
  }

  /**
    * In case there is a conflict, resolve it by adding a new version of the mapping table
    * and register the mapping between the old version and the new one in ahe entity version map.
    *
    * @param mappingTable A mapping table to save as an instance of Model 1 object
    * @param objectId     An Object Id if the mapping table
    * @param repo         An entity repository
    */
  def resolveConflict(mappingTable: model1.MappingTable, objectId: String, repo: EntityRepository): Unit = {
    var retriesLeft = EntityMigrator.NumberOfInsertRetries
    var saved = false
    while (retriesLeft > 0 && !saved) {
      retriesLeft -= 1
      val latestVersion = repo.getLatestVersion(mappingTable.name)
      val mappingTableToSave = mappingTable.copy(version = latestVersion + 1)
      val mappingTable1Json = ObjectIdTools
        .putObjectIdIfNotPresent(Serializer1.serializeMappingTable(mappingTableToSave), Option(objectId))

      try {
        repo.insertDocument(mappingTable1Json)
        evm.add(collectionBase, mappingTable.name, mappingTable.version, mappingTableToSave.version)
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
