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
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.{MigrationUtils, ObjectIdTools}
import za.co.absa.enceladus.migrations.migrations.model0
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0

import scala.util.control.NonFatal

/**
  * The class provides runs continuous migration from model version 0 to model version 1.
  *
  * @param evm         An entity mapper for tracking the mapping between old versions of entities and new ones.
  * @param databaseOld An instance of a MongoDB database connection containing old model documents.
  * @param databaseNew An instance of a MongoDB database connection containing new model documents.
  */
class MigratorRun(evm: EntityVersionMap,
                  databaseOld: MongoDatabase,
                  databaseNew: MongoDatabase) extends EntityMigrator {
  private val log: Logger = LogManager.getLogger(this.getClass)

  override protected val collectionBase = "run"
  protected val collectionOld: String = MigrationUtils.getVersionedCollectionName(collectionBase, 0)
  protected val collectionNew: String = MigrationUtils.getVersionedCollectionName(collectionBase, 1)

  protected val dbOld: MongoDatabase = databaseOld
  protected val dbNew: MongoDatabase = databaseNew

  /** Migration for runs is slightly different. */
  override def migrate(): Unit = {
    val repoOld = new EntityRepository(dbOld, collectionOld)
    val repoNew = new EntityRepository(dbNew, collectionNew)

    val runsOld = repoOld.getSortedRuns

    runsOld.foreach(runOld => {
      val objectId = ObjectIdTools.getObjectIdFromDocument(runOld)
      objectId.foreach(id => {
        if (!repoNew.doesDocumentExist(id)) {
          migrateEntity(runOld, id, repoNew)
        }
      })
    })
  }

  /**
    * Migrates a single instance of run.
    *
    * @param srcRunJson A JSON representation of a run in the model 0 format.
    * @param objectId   An Object Id of the run.
    * @param repo       An entity repository.
    */
  def migrateEntity(srcRunJson: String, objectId: String, repo: EntityRepository): Unit = {
    val runOpt = try {
      Option(migrateRun(Serializer0.deserializeRun(srcRunJson)))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for '$collectionBase': ${e.getMessage}")
        None
    }

    runOpt.foreach(run1 => {
      if (repo.doesRunExist(run1.runId, run1.dataset, run1.datasetVersion)) {
        resolveConflict(run1, objectId, repo)
      } else {
        normalInsert(run1, objectId, repo)
      }
    })
  }

  private def migrateRun(run0: model0.Run): model0.Run = {
    run0
      .copy(datasetVersion = evm.getSafeVersion("dataset", run0.dataset, run0.datasetVersion))
      .copy(controlMeasure = migrateControlMeasures(run0.controlMeasure))
  }

  private def migrateControlMeasures(cm: ControlMeasure): ControlMeasure = {
    cm.copy(metadata = cm.metadata
      .copy(additionalInfo = cm.metadata.additionalInfo.flatMap { case pair =>
        pair match {
          case ("raw_dir_size", v) => Seq(("std_input_dir_size", v))
          case ("publish_dir_size", v) => Seq(("conform_output_dir_size", v))
          case ("std_dir_size", v) => Seq(("std_output_dir_size", v), ("conform_input_dir_size", v))
          case p => Seq(p)
        }
      }))
  }

  /**
    * In case there is no conflict, insert a new dataset normally.
    * If inserting is unsuccessful fallback to conflict resolution.
    *
    * @param run      A dataset to save as an instance of Model 1 object
    * @param objectId An Object Id if the dataset
    * @param repo     An entity repository
    */
  def normalInsert(run: model0.Run, objectId: String, repo: EntityRepository): Unit = {
    val runJson = ObjectIdTools.putObjectIdIfNotPresent(Serializer0.serializeRun(run),
      Option(objectId))

    try {
      repo.insertDocument(runJson)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to append a document for '$collectionBase': ${e.getMessage}. Retrying...")
        resolveConflict(run, objectId, repo)
    }
  }

  /**
    * In case there is a conflict, resolve it by adding a new version of the dataset
    * and register the mapping between the old version and the new one in ahe entity version map.
    *
    * @param run      A run to save as an instance of Model 1 object
    * @param objectId An Object Id if the dataset
    * @param repo     An entity repository
    */
  def resolveConflict(run: model0.Run, objectId: String, repo: EntityRepository): Unit = {
    var retriesLeft = EntityMigrator.NumberOfInsertRetries
    var saved = false
    while (retriesLeft > 0 && !saved) {
      retriesLeft -= 1
      val latestRunId = repo.getLatestRunId(run.dataset, run.datasetVersion)
      val runToSave = run.copy(runId = latestRunId + 1)
      val runJson = ObjectIdTools
        .putObjectIdIfNotPresent(Serializer0.serializeRun(runToSave), Option(objectId))

      try {
        repo.insertDocument(runJson)
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
