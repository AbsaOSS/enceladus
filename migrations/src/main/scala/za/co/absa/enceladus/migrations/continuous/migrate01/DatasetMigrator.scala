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

import java.time.ZonedDateTime

import org.mongodb.scala.MongoDatabase
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.ObjectIdTools
import za.co.absa.enceladus.migrations.migrations.MigrationToV1.convertConformanceRule
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.Serializer1
import za.co.absa.enceladus.migrations.migrations.{model0, model1}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * The class provides datasets continuous migration from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
final class DatasetMigrator(evm: EntityVersionMap,
                            databaseOld: MongoDatabase,
                            databaseNew: MongoDatabase) extends EntityMigrator(databaseOld, databaseNew) {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  override protected val collectionBase: String = EntityMigrator.datasetCollection

  /**
    * Migrates a single instance of dataset.
    *
    * @param srcDatasetJson A JSON representation of a dataset in the model 0 format.
    * @param objectId       An Object Id of the dataset.
    * @param repo           An entity repository.
    */
  def migrateEntity(srcDatasetJson: String, objectId: String, repo: EntityRepository): Unit = {
    Try {
      val fixJson = srcDatasetJson.replaceAll("\"jsonClass\"\\s*:", "\"_t\" :")
      val dataset0 = Serializer0.deserializeDataset(fixJson)

      model1.Dataset(
        dataset0.name,
        dataset0.version,
        None,
        dataset0.hdfsPath,
        dataset0.hdfsPublishPath,
        dataset0.schemaName,
        evm.getSafeVersion(EntityMigrator.schemaCollection, dataset0.schemaName, dataset0.schemaVersion),
        ZonedDateTime.now(),
        migrationUserName,
        ZonedDateTime.now(),
        migrationUserName,
        conformance = dataset0.conformance.map(migrateConformanceRule)
      )
    } match {
      case Success(dataset1) =>
        if (repo.doesDocumentExist(dataset1.name, dataset1.version)) {
          resolveConflict(dataset1, objectId, repo)
        } else {
          normalInsert(dataset1, objectId, repo)
        }
      case Failure(e) =>
        log.error(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
    }
  }

  /**
    * Migrates a conformance rule replacing a foreign key reference if necessary.
    * Only mapping rules have foreign key references in the model version 0.
    *
    * @param rule A conformance rule from the model version 0.
    * @return The transformed conformance rule.
    */
  def migrateConformanceRule(rule: model0.conformanceRule.ConformanceRule): model1.conformanceRule.ConformanceRule = {
    val migratedConformanceRule = convertConformanceRule(rule)
    migratedConformanceRule match {
      case r: model1.conformanceRule.MappingConformanceRule =>
        r.copy(mappingTableVersion = evm.getSafeVersion(EntityMigrator.mappingTableCollection,
          r.mappingTable,
          r.mappingTableVersion))
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
    * and register the mapping between the old version and the new one in the entity version map.
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
            log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}. Retrying...")
          } else {
            throw e // Something went terribly wrong
          }
      }
    }
  }

}
