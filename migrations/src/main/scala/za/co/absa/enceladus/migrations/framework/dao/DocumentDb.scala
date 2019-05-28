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

package za.co.absa.enceladus.migrations.framework.dao

/**
  * This abstract class is the contract a document database need to implement in order to run migrations.
  */
abstract class DocumentDb {

  def getVersion(): Int

  def setVersion(version: Int): Unit

  def isCollectionExists(collectionName: String): Boolean

  def createCollection(collectionName: String): Unit

  def dropCollection(collectionName: String): Unit

  def emptyCollection(collectionName: String): Unit

  def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit

  def cloneCollection(collectionName: String, newCollectionName: String): Unit

  def insertDocument(collectionName: String, document: String): Unit

  def executeQuery(query: String)

  def getDocuments(collectionName: String): Iterator[String]
}
