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
import za.co.absa.enceladus.migrations.framework.{MigrationUtils, ObjectIdTools}

object EntityMigrator {
  // Specifies the number of retries for inserting a new version of an entity into a database.
  val NumberOfInsertRetries = 3

  val schemaCollection = "schema"
  val mappingTableCollection = "mapping_table"
  val datasetCollection = "dataset"
  val runCollection = "run"
}

/**
  * An base class for continuous migration providers.
  * @param databaseOld A database to migrate from.
  * @param databaseNew A database to migrate to.
  */
abstract class EntityMigrator(databaseOld: MongoDatabase,
                              databaseNew: MongoDatabase) {

  protected val migrationUserName = "c_migration"

  /** A collection base name. E.g. 'schema' or 'dataset' */
  protected def collectionBase: String

  /** A versioned collection name for the old version of the model. E.g. 'schema' or 'dataset' */
  protected lazy val collectionOld: String = MigrationUtils.getVersionedCollectionName(collectionBase, 0)

  /** A versioned collection name for the new version of the model. E.g. 'schema_v1' or 'dataset_v1' */
  protected lazy val collectionNew: String = MigrationUtils.getVersionedCollectionName(collectionBase, 1)

  protected val dbOld: MongoDatabase = databaseOld
  protected val dbNew: MongoDatabase = databaseNew

  /** Migrate a specific entity. This should be overridden an implemented in concrete classes */
  def migrateEntity(srcJson: String, objectId: String, repo: EntityRepository): Unit

  /** Runs a continuous migration for schemas. */
  def migrate(): Unit = {
    val repoOld = new EntityRepository(dbOld, collectionOld)
    val repoNew = new EntityRepository(dbNew, collectionNew)

    val entitiesOld = repoOld.getSortedDocuments

    entitiesOld.foreach(entityOld => {
      val objectId = ObjectIdTools.getObjectIdFromDocument(entityOld)
      objectId.foreach(id => {
        if (!repoNew.doesDocumentExist(id)) {
          migrateEntity(entityOld, id, repoNew)
        }
      })
    })
  }

}
