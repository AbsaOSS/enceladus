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

package za.co.absa.enceladus.migrations.continuous

/**
  * A base class representing en entity version map. Entity version map is a tool for keeping track
  * of 'name - version' mapping of entities between different versions of the database.
  *
  * It is used for a conflict resolution when doing a continuous migration.
  * A conflict happens when an entity from the old version of the database has an object id that does not exist
  * in the new version of the database, but the ‘name - version’ pair of the entity already exists.
  * Indexes should not allow for ‘name - version’ duplicates to be created.
  *
  * * When a conflict happens a new version of the imported entity is created and the mapping between the entity
  *   in the old database and the new one is created.
  * * When importing an entity that has a reference to another entity remap the 'foreign key' according to the map.
  */
abstract class EntityVersionMap {

  /**
    * Adds a 'name - version' mapping.
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @param newVersion     An version of the entity in the new version of the database
    */
  @throws[IllegalStateException]
  final def add(collectionName: String, entityName: String, oldVersion: Int, newVersion: Int): Unit = {
    get(collectionName, entityName, oldVersion) match {
      case Some(storedVersion) =>
        if (storedVersion != newVersion) {
          throw new IllegalStateException("Attempt to overwrite an entity-version mapping. " +
          s"collection = '$collectionName', entity = '$entityName', oldVersion = '$oldVersion', " +
            s"newVersion = '$newVersion'. Version stored = $storedVersion")
        }
      case None =>
        addEntry(collectionName, entityName, oldVersion, newVersion)
    }
  }

  /**
    * An implementation if this abstract class should redefine this method. Clients of this class shoudl use add()
    * as it does additional checks.
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @param newVersion     An version of the entity in the new version of the database
    */
  protected def addEntry(collectionName: String, entityName: String, oldVersion: Int, newVersion: Int): Unit

  /**
    * Gets a 'name - version' mapping.
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An Entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @return An version of the entity in the new version of the database, None if the entity is not found
    *         in the mapping
    */
  @throws[IllegalStateException]
  def get(collectionName: String, entityName: String, oldVersion: Int): Option[Int]

  /**
    * Gets a safe 'name - version' mapping. If the maping isn't found in the entity version map it is assumed
    * the original version number can be used
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An Entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @return An version of the entity in the new version of the database, None if the entity is not found
    *         in the mapping
    */
  def getSafeVersion(collectionName: String, entityName: String, oldVersion: Int): Int = {
    get(collectionName, entityName, oldVersion).getOrElse(oldVersion)
  }
}
