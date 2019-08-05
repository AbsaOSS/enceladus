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
import za.co.absa.enceladus.migrations.migrations.MigrationToV1.convertConformanceRule
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.Serializer1
import za.co.absa.enceladus.migrations.migrations.{model0, model1}

import scala.util.control.NonFatal

/**
  * The class provides datasets continuous migration from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
class MigratorDataset(evm: EntityVersionMap,
                      databaseOld: MongoDatabase,
                      databaseNew: MongoDatabase) extends EntityMigrator {
  private val log: Logger = LogManager.getLogger(this.getClass)

  override protected val collectionBase = "dataset"
  protected val collectionOld: String = MigrationUtils.getVersionedCollectionName(collectionBase, 0)
  protected val collectionNew: String = MigrationUtils.getVersionedCollectionName(collectionBase, 1)

  protected val dbOld: MongoDatabase = databaseOld
  protected val dbNew: MongoDatabase = databaseNew

  /**
    * Migrates a single instance of dataset.
    *
    * @param srcDatasetJson A JSON representation of a dataset in the model 0 format.
    * @param objectId       An Object Id of the dataset.
    * @param repo           An entity repository.
    */
  def migrateEntity(srcDatasetJson: String, objectId: String, repo: EntityRepository): Unit = {
    val dataset1Opt = try {
      val fixJson = srcDatasetJson.replaceAll("\"jsonClass\" :", "\"_t\" :")
      val dataset0 = Serializer0.deserializeDataset(fixJson)

      Option(model1.Dataset(
        dataset0.name,
        dataset0.version,
        None,
        dataset0.hdfsPath,
        dataset0.hdfsPublishPath,
        dataset0.schemaName,
        evm.getSafeVersion("schema", dataset0.schemaName, dataset0.schemaVersion),
        ZonedDateTime.now(),
        migrationUserName,
        ZonedDateTime.now(),
        migrationUserName,
        conformance = dataset0.conformance.map(migrateConformanceRule)
      ))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
        None
    }

    dataset1Opt.foreach(dataset1 => {
      if (repo.doesDocumentExist(dataset1.name, dataset1.version)) {
        resolveConflict(dataset1, objectId, repo)
      } else {
        normalInsert(dataset1, objectId, repo)
      }
    })
  }

  def migrateConformanceRule(rule: model0.conformanceRule.ConformanceRule): model1.conformanceRule.ConformanceRule = {
    val migratedConformanceRule = convertConformanceRule(rule)
    migratedConformanceRule match {
      case r: model1.conformanceRule.MappingConformanceRule =>
        r.copy(mappingTableVersion = evm.getSafeVersion("mapping_table", r.mappingTable, r.mappingTableVersion))
      case r =>
        r
    }
  }

  /**
    * In case there is no conflict, insert a new dataset normally.
    * If inserting is unsuccessful fallback to conflict resolution.
    *
    * @param dataset  A dataset to save as an instance of Model 1 object
    * @param objectId An Object Id if the dataset
    * @param repo     An entity repository
    */
  def normalInsert(dataset: model1.Dataset, objectId: String, repo: EntityRepository): Unit = {
    val dataset1Json = ObjectIdTools.putObjectIdIfNotPresent(Serializer1.serializeDataset(dataset),
      Option(objectId))

    try {
      repo.insertDocument(dataset1Json)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}. Retrying...")
        resolveConflict(dataset, objectId, repo)
    }
  }

  /**
    * In case there is a conflict, resolve it by adding a new version of the dataset
    * and register the mapping between the old version and the new one in ahe entity version map.
    *
    * @param dataset  A dataset to save as an instance of Model 1 object
    * @param objectId An Object Id if the dataset
    * @param repo     An entity repository
    */
  def resolveConflict(dataset: model1.Dataset, objectId: String, repo: EntityRepository): Unit = {
    var retriesLeft = EntityMigrator.NumberOfInsertRetries
    var saved = false
    while (retriesLeft > 0 && !saved) {
      retriesLeft -= 1
      val latestVersion = repo.getLatestVersion(dataset.name)
      val datasetToSave = dataset.copy(version = latestVersion + 1)
      val dataset1Json = ObjectIdTools
        .putObjectIdIfNotPresent(Serializer1.serializeDataset(datasetToSave), Option(objectId))

      try {
        repo.insertDocument(dataset1Json)
        evm.add(collectionBase, dataset.name, dataset.version, datasetToSave.version)
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
