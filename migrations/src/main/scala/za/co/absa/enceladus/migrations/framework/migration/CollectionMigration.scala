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

package za.co.absa.enceladus.migrations.framework.migration

import za.co.absa.enceladus.migrations.framework.MigrationUtils
import za.co.absa.enceladus.migrations.framework.dao.DocumentDb

import scala.collection.mutable.ListBuffer

/**
  * A CollectionMigration represents an entity that provides ability to add, rename and remove collections
  *
  * In order to create a collection migration you need to extend from this trait and provide the requested
  * collection changes:
  *
  * {{{
  *   class MigrationTo1 extends MigrationBase with CollectionMigration {
  *
  *     addCollection("collection1_name")
  *     addCollection("collection2_name")
  *     addCollection("collection3_name")
  *
  *     removeCollection("collection4_name")
  *     removeCollection("collection5_name")
  *     removeCollection("collection6_name")
  *   }
  * }}}
  */
trait CollectionMigration extends Migration {

  /**
    * This method is used by derived classes to add new collection as a step of migration process.
    *
    * @param collectionName A collection name to be added
    */
  def addCollection(collectionName: String): Unit = {
    if (collectionsToAdd.contains(collectionName)) {
      throw new IllegalArgumentException(s"Collection '$collectionName' is already added.")
    }
    if (collectionsToRemove.contains(collectionName)) {
      throw new IllegalArgumentException(s"Cannot both add and remove '$collectionName' as a migration step.")
    }
    collectionsToAdd += collectionName
  }

  /**
    * This method is used by derived classes to remove a collection as a step of migration process.
    *
    * @param collectionName A collection name to be added
    */
  def removeCollection(collectionName: String): Unit = {
    if (collectionsToRemove.contains(collectionName)) {
      throw new IllegalArgumentException(s"Collection '$collectionName' is already in the removal list.")
    }
    if (collectionsToAdd.contains(collectionName)) {
      throw new IllegalArgumentException(s"Cannot both add and remove '$collectionName' as a migration step.")
    }
    collectionsToRemove += collectionName
  }

  /**
    * This method is used by derived classes to rename a collection as a step of migration process.
    *
    * @param oldName A collection to be renamed
    * @param newName A new name for the collection
    */
  def renameCollection(oldName: String, newName: String): Unit = {
    if (collectionsToRemove.contains(oldName)) {
      throw new IllegalArgumentException(s"Collection '$oldName' is in the removal list.")
    }
    if (collectionsToAdd.contains(oldName)) {
      throw new IllegalArgumentException(s"Collection '$oldName' is in the list of new collections. Cannot rename it.")
    }
    collectionsToRename += oldName -> newName
  }

  /** Returns a list of collections to be added during the migration */
  def getCollectionsToAdd: List[String] = collectionsToAdd.toList

  /** Returns a list of collections to be removed during the migration */
  def getCollectionsToRemove: List[String] = collectionsToRemove.toList

  /** Returns a list of collections to be removed during the migration */
  def getCollectionsToRename: List[(String, String)] = collectionsToRename.toList

  override def applyCollectionChanges(collections: List[String]): List[String] = {
    var newCollections = collections
    for (c <- getCollectionsToAdd) {
      if (!newCollections.contains(c)) {
        newCollections = newCollections :+ c
      }
    }
    val collectionsToRemove = getCollectionsToRemove
    newCollections = newCollections.filterNot(c => collectionsToRemove.contains(c))

    val renameMap = collectionsToRename.toMap
    newCollections.map(collectionName => {
      renameMap.getOrElse(collectionName, collectionName)
    })
  }

  /**
    * Executes a migration on a given database and a list of collection names.
    */
  abstract override def execute(db: DocumentDb, collectionNames: Seq[String]): Unit = {
    super.execute(db, collectionNames)
    collectionsToAdd.foreach(c => db.createCollection(MigrationUtils.getVersionedCollectionName(c, targetVersion)))
    collectionsToRemove.foreach(c => db.dropCollection(MigrationUtils.getVersionedCollectionName(c, targetVersion)))
    collectionsToRename.foreach {
      case (oldName, newName) =>
        db.renameCollection(MigrationUtils.getVersionedCollectionName(oldName, targetVersion),
          MigrationUtils.getVersionedCollectionName(newName, targetVersion))
    }
  }

  /**
    * Validate the possibility of running a migration given a list of collection names.
    */
  abstract override def validate(collectionNames: Seq[String]): Unit = {
    super.validate(collectionNames)
    collectionsToAdd.foreach(collectionToMigrate =>
      if (!collectionNames.contains(collectionToMigrate)) {
        throw new IllegalStateException(
          s"Attempt to add a collection that already exists in db version ${targetVersion - 1}: $collectionToMigrate.")
      }
    )
    collectionsToRemove.foreach(collectionToMigrate =>
      if (!collectionNames.contains(collectionToMigrate)) {
        throw new IllegalStateException(
          s"Attempt to drop a collection that does not exist in db version ${targetVersion - 1}: $collectionToMigrate.")
      }
    )
    collectionsToRename.foreach {
      case (oldName, newName) =>
        if (!collectionNames.contains(oldName)) {
          throw new IllegalStateException(
            s"Attempt to rename a collection that does not exist: $oldName.")
        }
        if (!collectionNames.contains(newName)) {
          throw new IllegalStateException(
            s"Attempt to rename a collection to a one that already exists in db version ${targetVersion - 1}: " +
              s"$newName.")
        }
        if (collectionsToAdd.contains(oldName)) {
          throw new IllegalStateException(
            s"Cannot add and rename a collection as a part of single migration in db version ${targetVersion - 1}: " +
              s"$oldName.")
        }
        if (collectionsToAdd.contains(newName)) {
          throw new IllegalStateException(
            s"Cannot add and rename a collection as a part of single migration in db version ${targetVersion - 1}: " +
              s"$newName.")
        }
        if (collectionsToRemove.contains(oldName)) {
          throw new IllegalStateException(
            s"Cannot drop and rename a collection as a part of single migration in db version ${targetVersion - 1}: " +
              s"$oldName.")
        }
        if (collectionsToRemove.contains(newName)) {
          throw new IllegalStateException(
            s"Cannot drop and rename a collection as a part of single migration in db version ${targetVersion - 1}: " +
              s"$newName.")
        }
    }
  }

  override protected def validateMigration(): Unit = {
    if (targetVersion < 0) {
      throw new IllegalStateException("The target version of a CollectionMigration should be 0 or bigger.")
    }
  }

  private val collectionsToAdd = new ListBuffer[String]()
  private val collectionsToRemove = new ListBuffer[String]()
  private val collectionsToRename = new ListBuffer[(String, String)]()
}
