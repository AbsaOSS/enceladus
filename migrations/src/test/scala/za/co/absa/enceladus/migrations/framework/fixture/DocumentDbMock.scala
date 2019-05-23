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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * This version of a document DB implements all methods required, but does not persist the data.
  */
class DocumentDbMock extends DocumentDb {
  override def collectionExists(collectionName: String): Boolean = db.contains(collectionName)

  override def createCollection(collectionName: String): Unit = {
    if (collectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection already exists: '$collectionName'.")
    }
    db.put(collectionName, new ArrayBuffer[String]())
  }

  override def dropCollection(collectionName: String): Unit = {
    if (!collectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    db.remove(collectionName)
  }

  override def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit = {
    if (!collectionExists(collectionNameOld)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionNameOld'.")
    }
    if (collectionExists(collectionNameNew)) {
      throw new IllegalStateException(s"Collection already exists: '$collectionNameNew'.")
    }
    val docs = db(collectionNameOld)
    db.remove(collectionNameOld)
    db.put(collectionNameNew, docs)
  }

  override def cloneCollection(collectionName: String, newCollectionName: String): Unit = {
    if (!collectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    if (collectionExists(newCollectionName)) {
      throw new IllegalStateException(s"Collection already exists: '$newCollectionName'.")
    }
    val documents = db(collectionName)
    val documentsCopy = new ArrayBuffer[String]()
    documentsCopy.insertAll(0, documents)
    db.put(newCollectionName, documentsCopy)
  }

  override def insertDocument(collectionName: String, document: String): Unit = {
    if (!collectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    val documents = db(collectionName)
    documents += document
  }

  override def executeQuery(query: String): Unit = {}

  override def getDocuments(collectionName: String): Iterator[String] = {
    if (!collectionExists(collectionName)) {
      throw new IllegalStateException(s"Collection does not exist: '$collectionName'.")
    }
    db(collectionName).toIterator
  }

  override def getVersion(): Int = version

  override def setVersion(version: Int): Unit = this.version = version

  private val db = new mutable.HashMap[String, ArrayBuffer[String]]()
  private var version = 0
}
