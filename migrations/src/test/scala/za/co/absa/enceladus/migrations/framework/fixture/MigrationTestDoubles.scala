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

package za.co.absa.enceladus.migrations.framework.fixture

import za.co.absa.enceladus.migrations.framework.dao.DocumentDb
import za.co.absa.enceladus.migrations.framework.migration.IndexField

object MigrationTestDoubles {

  object DocumentDbStub extends DocumentDb {
    override def getVersion(): Int = 0
    override def setVersion(version: Int): Unit = {}
    override def dropCollection(collectionName: String): Unit = {}
    override def emptyCollection(collectionName: String): Unit = {}
    override def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit = {}
    override def doesCollectionExists(collectionName: String): Boolean = true
    override def createCollection(collectionName: String): Unit = {}
    override def cloneCollection(collectionName: String, newCollectionName: String): Unit = {}
    override def createIndex(collectionName: String, keys: Seq[IndexField], unique: Boolean = false): Unit = {}
    override def dropIndex(collectionName: String, keys: Seq[IndexField]): Unit = {}
    override def getDocumentsCount(collectionName: String): Long = 0
    override def insertDocument(collectionName: String, document: String): Unit = {}
    override def executeCommand(query: String): Unit = {}
    override def getDocuments(collectionName: String): Iterator[String] = List[String]().toIterator
  }
}
