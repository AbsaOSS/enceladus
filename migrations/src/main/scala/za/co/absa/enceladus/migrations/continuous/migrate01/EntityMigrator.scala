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
import za.co.absa.enceladus.migrations.framework.ObjectIdTools

/**
  * An base class for continuous migration providers.
  */
abstract class EntityMigrator {

  protected val migrationUserName = "c_migration"

  /** A collection base name. E.g. 'schema' or 'dataset' */
  protected def collectionBase: String

  /** A versioned collection name for the old version of the mode. E.g. 'schema_v1' or 'dataset_v1' */
  protected def collectionOld: String

  /** A versioned collection name for the old version of the mode. E.g. 'schema_v0' or 'dataset_v0' */
  protected def collectionNew: String

  protected def dbOld: MongoDatabase
  protected def dbNew: MongoDatabase

  def migrateEntity(srcJson: String, objectId: String, repo: EntityEepository): Unit

  /** Runs a continuous migration for schemas. */
  def migrate(): Unit = {
    val repoOld = new EntityEepository(dbOld, collectionOld)
    val repoNew = new EntityEepository(dbNew, collectionNew)

    val schemasOld = repoOld.getSortedDocuments

    schemasOld.foreach(schemaOld => {
      val objectId = ObjectIdTools.getObjectIdFromDocument(schemaOld)
      objectId.foreach(id => {
        if (!repoNew.doesDocumentExist(id)) {
          migrateEntity(schemaOld, id, repoNew)
        }
      })
    })
  }

}

object EntityMigrator {
  // Specifies the number of retries for inserting a new version of an entity into a database.
  val NumberOfInsertRetries = 3
}
