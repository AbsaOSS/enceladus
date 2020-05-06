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

package za.co.absa.enceladus.migrations.framework

import org.slf4j.{Logger, LoggerFactory}

import util.control.Breaks._
import za.co.absa.enceladus.migrations.framework.dao.DocumentDb
import za.co.absa.enceladus.migrations.framework.migration._

import scala.collection.mutable.ArrayBuffer

class Migrator(db: DocumentDb, migrations: Seq[Migration]) {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getCollectionMigrations: Seq[CollectionMigration] = migrations.collect({ case m: CollectionMigration => m })

  def getQueryMigrations: Seq[CommandMigration] = migrations.collect({ case m: CommandMigration => m })

  def getJsonMigrations: Seq[JsonMigration] = migrations.collect({ case m: JsonMigration => m })

  /**
    * Checks if a database is empty
    *
    * - If there is 'db_version' collection in the database, it is definitely not empty.
    * - If there is at least one of initial version 0 collections in the database => it is not empty
    */
  def isDatabaseEmpty(): Boolean = {
    if (db.doesCollectionExists(Configuration.DatabaseVersionCollectionName)) {
      false
    } else {
      val collections = getCollectionNames(0)
      collections.forall(c => !db.doesCollectionExists(c))
    }
  }

  /**
    * Returns if a migration is required given the expected model version of the database schema.
    */
  def isMigrationRequired(expectedModelVersion: Int): Boolean = {
    val currentDbVersion = db.getVersion()
    expectedModelVersion > currentDbVersion
  }

  /**
    * Initializes the database of the specified version. Creates 'db_version', all collections and indexes
    */
  def initializeDatabase(targetDbVersion: Int): Unit = {
    validate(targetDbVersion)

    val collections = getCollectionNames(targetDbVersion)
    collections.foreach(c => db.createCollection(MigrationUtils.getVersionedCollectionName(c, targetDbVersion)))

    val indexes = getIndexes(targetDbVersion)
    indexes.foreach { case Index(c, keys, unique, sparse) =>
      db.createIndex(MigrationUtils.getVersionedCollectionName(c, targetDbVersion), keys, unique, sparse)
    }

    db.setVersion(targetDbVersion)
  }

  /**
    * ==Runs a sequence of migrations==
    * Does the migration from the current version of the database to the target one.
    *
    * The migrations passed into the constructor should be:
    * <ul>
    * <li>In the order of database versions</li>
    * <li>Without gaps in version numbers</li>
    * <li>There should be only one migration per version switch</li>
    * </ul>
    *
    * ==Migrations are non-destructive==
    * Migrations are not destructive, they are safe to run on existing databases. Model v0 collections are expected to
    * have no postfix, e.g. 'dataset', 'schema'. The model v1 collections are expected to have a version postfix, e.g.
    * 'dataset_v1', 'schema_v1'. Several models can coexist in one database sharing all MongoDB connection settings.
    * The latest usable model is specified in 'db_version' collection.
    *
    * ==Object Id retention==
    * Object Ids are retained during migrations which opens a possibility to later have a continuous migration process.
    *
    * ==Interrupt tolerance==
    * Migrations are interruption tolerant. The last thing a migration does is writing a new version into 'db_version'.
    * If a process was interrupted before completion the version in 'db_version' will still be old. When a new migration
    * starts it will drop all partially migrated collections and will start a migration from the beginning.
    *
    * @param targetDbVersion A version of the database to migrate to.
    */
  def migrate(targetDbVersion: Int): Unit = {
    validate(targetDbVersion)

    val sourceDbVersion = db.getVersion()
    validateDbVersions(sourceDbVersion, targetDbVersion)

    var currentVersionCollections = getCollectionNames(sourceDbVersion)

    if (currentVersionCollections.isEmpty) {
      throw new IllegalStateException(s"No collection names are registered for db version $sourceDbVersion.")
    }

    for (i <- sourceDbVersion + 1 to targetDbVersion) {
      cleanUpUnfinishedMigrations(db, currentVersionCollections, i)

      val mig = migrations(i)
      mig.execute(db, currentVersionCollections)
      db.setVersion(i)
      currentVersionCollections = mig.applyCollectionChanges(currentVersionCollections)
    }
  }

  /**
    * Get the list of collection names valid for a particular version of the database.
    *
    * @param dbVersion A version number of the database.
    */
  def getCollectionNames(dbVersion: Int): List[String] = {
    val collectionMigrations = getCollectionMigrations
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

  /**
    * Get the list of indexes that are expected to be at the target db version
    *
    * @param dbVersion A version number of the database.
    */
  def getIndexes(dbVersion: Int): Seq[Index] = {
    val indexes = new ArrayBuffer[Index]()

    def applyIndexChanges(c: CollectionMigration): Unit = {
      c.getIndexesToRemove.foreach(idx => {
        val i = indexes.indexOf(idx)
        if (i >= 0) {
          indexes.remove(i)
        }
      })

      c.getIndexesToAdd.foreach(idx => {
        val i = indexes.indexOf(idx)
        if (i < 0) {
          indexes += idx
        }
      })
    }

    migrations.foreach {
      case c: CollectionMigration =>
        applyIndexChanges(c)
      case _ =>
      // Nothing to do
    }
    indexes
  }


  /**
    * Validates migrations for self consistency.
    *
    * - Target version numbers should be consequent
    * - Collections should be added, removed or renamed consistently
    * - Migrations should refer only to collections that do exist
    * - Validate that migration path exists from version 0 to `targetDbVersion`
    *
    * @param targetDbVersion A version of the database that should be reachable by applying migrations
    */
  def validate(targetDbVersion: Int): Unit = {
    validateVersionNumbersConsequent()
    validateTargetVersion(targetDbVersion)
    validateCollectionManipulationConsistency(targetDbVersion)
  }

  /**
    * Validates that a list of migrations have target database versions in ascending order and there are no gaps
    * between version numbers.
    */
  private def validateVersionNumbersConsequent(): Unit = {
    migrations.zipWithIndex.foreach{ case(m, i) =>
      val v = m.targetVersion
      if (v < 0 ) {
        throw new IllegalStateException(s"A negative ($v) target version is encountered in a migration spec.")
      }
      if (v - i > 0) {
        throw new IllegalStateException(s"The list of migrations jumps from version $i to $v.")
      }
      if (v < i - 1) {
        throw new IllegalStateException(s"The the migrations for version $i and version $v are out of order.")
      }
      if (v == i - 1) {
        throw new IllegalStateException(s"Found 2 migrations for the same target version $i.")
      }
    }
  }

  /**
    * Validates that a list of migrations manipulate collections consistently, e.g.
    *  - A migration for version 0 should provide the initial list of collections
    *  - Same collections should not be added and removed inside a single migration
    *  - Only existing collections can be removed or renamed
    *  - Collections should not be added twice
    */
  private def validateCollectionManipulationConsistency(targetDbVersion: Int): Unit = {
    var currentVersionCollections = getCollectionNames(0)

    if (currentVersionCollections.isEmpty) {
      throw new IllegalStateException("No collection names are registered for db version 0.")
    }

    for (i <- 1 to targetDbVersion) {
      val mig = migrations(i)
      mig.validate(currentVersionCollections)
      currentVersionCollections = mig.applyCollectionChanges(currentVersionCollections)
    }
  }

  /**
    * Validates that a target database version is reachable through migration steps.
    */
  private def validateTargetVersion(targetDbVersion: Int): Unit = {
    if (!migrations.exists(_.targetVersion == targetDbVersion)) {
      throw new IllegalStateException(s"The target database version ($targetDbVersion) is not reachable.")
    }
  }

  /**
    * Validates that values of a source and a target database versions make sense.
    */
  private def validateDbVersions(sourceDbVersion: Int, targetDbVersion: Int): Unit = {
    if (sourceDbVersion < 0) {
      throw new IllegalStateException(s"Source database version cannot be negative.")
    }

    if (targetDbVersion < sourceDbVersion) {
      throw new IllegalStateException(s"A target database version cannot be less than a source version.")
    }
  }

  /**
    * Removes collections that were created during an interrupted migration process.
    *
    * Each migration always finishes with setting up the current database version in 'db_version'.
    * If the value in 'db_version' is, say, 5, but there are collections in the database with '_v6' postfix,
    * e.g. 'dataset_v6', than it is assumed that a migration from v5 to v6 did not finish successfully.
    * Collections created during that process will be dropped.
    */
  private def cleanUpUnfinishedMigrations(db: DocumentDb, collections: List[String], targetVersion: Int): Unit = {
    // Cleaning up cloned and not processed collections
    dropCollections(db, collections, targetVersion)

    // Cleaning up partially processed collections
    val migrationsToExecute = migrations.filter(m => m.targetVersion == targetVersion)
    var migratedCollections = collections
    migrationsToExecute.foreach(m => {
      migratedCollections = m.applyCollectionChanges(migratedCollections)
      dropCollections(db, collections, targetVersion)
    })
  }

  /**
    * Drops collections from the specified list and the specified database version number.
    */
  private def dropCollections(db: DocumentDb, collections: List[String], dbVersion: Int): Unit = {
    collections
      .map(c => MigrationUtils.getVersionedCollectionName(c, dbVersion))
      .foreach(collection => if (db.doesCollectionExists(collection)) {
        log.info(s"Dropping partially migrated collection $collection")
        db.dropCollection(collection)
      })
  }

}
