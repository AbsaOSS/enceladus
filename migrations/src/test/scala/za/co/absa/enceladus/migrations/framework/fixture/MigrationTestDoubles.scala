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

object MigrationTestDoubles {

  object DocumentDbStub extends DocumentDb {
    override def collectionExists(collectionName: String): Boolean = true
    override def createCollection(collectionName: String): Unit = {}
    override def cloneCollection(collectionName: String, newCollectionName: String): Unit = {}
    override def insertDocument(collectionName: String, document: String): Unit = {}
    override def executeQuery(query: String): Unit = {}
    override def getDocuments(collectionName: String): Iterator[String] = List[String]().toIterator
  }
}
