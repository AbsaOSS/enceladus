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

package za.co.absa.enceladus.migrations.framework.continuous.fixture

import za.co.absa.enceladus.migrations.continuous.EntityVersionMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EntityVersionMapMock extends EntityVersionMap {

  /**
    * Adds a 'name - version' mapping.
    *
    * @param collectionName The name of the collection that containg the entity
    * @param entityName     An Entity name
    * @param oldVersion     An version of the entity in the old version of the database
    * @param newVersion     An version of the entity in the new version of the database
    */
  override def addEntry(collectionName: String, entityName: String, oldVersion: Int, newVersion: Int): Unit = {
    db.put((collectionName, entityName, oldVersion), newVersion)
    actionsExecuted += s"EntityVersionMapMock.put($collectionName,$entityName,$oldVersion,$newVersion)"
  }

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
  override def get(collectionName: String, entityName: String, oldVersion: Int): Option[Int] = {
    val versionOpt = db.get((collectionName, entityName, oldVersion))
    actionsExecuted += s"EntityVersionMapMock.get($collectionName,$entityName,$oldVersion) = $versionOpt"
    versionOpt
  }

  def getActionsExecuted: List[String] = actionsExecuted.toList

  def resetExecutedActions(): Unit = {
    actionsExecuted.clear()
  }

  type LookupType = (String, String, Int)
  private val db = new mutable.HashMap[LookupType, Int]()
  private val actionsExecuted = new ListBuffer[String]
}
