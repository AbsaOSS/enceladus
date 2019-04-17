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

package za.co.absa.enceladus.migrations.framework

import util.control.Breaks._
import za.co.absa.enceladus.migrations.framework.dao.DocumentDb
import za.co.absa.enceladus.migrations.framework.migration.{CollectionMigration, JsonMigration, Migration, QueryMigration}

class Migrator(db: DocumentDb, migrations: Seq[Migration]) {

  def getCollectionMigrations: Seq[CollectionMigration] = migrations.collect({case m: CollectionMigration => m})

  def getQueryMigrations: Seq[QueryMigration] = migrations.collect({case m: QueryMigration => m})

  def getJsonMigrations: Seq[JsonMigration] = migrations.collect({case m: JsonMigration => m})

  /**
    * Do the migration from a specified version of the database to the target one.
    *
    * @param sourceDbVersion A version of the database to migrate from
    * @param targetDbVersion A version of the database to migrate to
    */
  def migrate(sourceDbVersion: Int, targetDbVersion: Int): Unit = {
    validateDbVersios(sourceDbVersion, targetDbVersion)

    var currentVersionCollections = getCollectionNames(sourceDbVersion)

    if (currentVersionCollections.isEmpty) {
      throw new IllegalStateException(s"No collection names are registered for db version $sourceDbVersion.")
    }

    if (targetDbVersion > sourceDbVersion) {
      for (i <- sourceDbVersion until targetDbVersion) {
        val migrationsToExecute = migrations.filter(m => m.targetVersion == i)
        migrationsToExecute.foreach(_.execute(db, currentVersionCollections))
        migrationsToExecute.foreach(m =>
          currentVersionCollections = m.applyCollectionChanges(currentVersionCollections))
      }
    }
  }

  def getCollectionNames(dbVersion: Int): List[String] = {
    val collectionMigrations = getCollectionMigrations.sortBy(_.targetVersion)
    var collections: List[String] = Nil
    breakable {
      collectionMigrations.foreach(m => {
        if (m.targetVersion > dbVersion) {
          break
        } else {
          collections = m.applyCollectionChanges(collections)
        }
      })
    }
    collections
  }

  private def validateDbVersios(sourceDbVersion: Int, targetDbVersion: Int): Unit = {
    if (sourceDbVersion < 0) {
      throw new IllegalStateException(s"Source database version cannot be negative.")
    }

    if (targetDbVersion < sourceDbVersion) {
      throw new IllegalStateException(s"A target database version cannot be less than a source version.")
    }
  }


}
