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

package za.co.absa.enceladus.migrations.framework.continuous.fixture

import za.co.absa.enceladus.migrations.continuous.EntityVersionMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EntityVersionMapMock extends EntityVersionMap {

  type LookupType = (String, String, Int)
  private val db = new mutable.HashMap[LookupType, Int]()
  private val actionsExecuted = new ListBuffer[String]

  override def addEntry(collectionName: String, entityName: String, oldVersion: Int, newVersion: Int): Unit = {
    db.put((collectionName, entityName, oldVersion), newVersion)
    actionsExecuted += s"EntityVersionMapMock.put($collectionName,$entityName,$oldVersion,$newVersion)"
  }

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
}
