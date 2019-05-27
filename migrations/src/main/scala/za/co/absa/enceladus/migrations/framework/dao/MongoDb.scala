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

import java.util.concurrent.TimeUnit

import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.mongodb.scala.{Completed, FindObservable, MongoCollection, MongoDatabase, Observable, SingleObservable}
import za.co.absa.enceladus.migrations.framework.model.DbVersion

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

class MongoDb (db: MongoDatabase) extends DocumentDb {

  val dbVersionCollectionName = "db_version"
  val queryTimeout = 30
  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[DbVersion]), DEFAULT_CODEC_REGISTRY)

  override def getVersion(): Int = {
    if (!isCollectionExists(dbVersionCollectionName)) {
      synchronousCallSingle(db.createCollection(dbVersionCollectionName))
      synchronousCallSingle(getCollection[DbVersion](dbVersionCollectionName)
        .insertOne(DbVersion(0)))
    }

    val versions = synchronousCallFind(getCollection(dbVersionCollectionName).find[DbVersion]())
    if (versions.lengthCompare(1) != 0) {
      val len = versions.length
      throw new IllegalStateException(
        s"Unexpected number of documents in '$dbVersionCollectionName'. Expected 1, got $len")
    }

    versions.head.version
  }

  override def setVersion(version: Int): Unit = ???

  override def isCollectionExists(collectionName: String): Boolean = {
    val collections = synchronousCall(db.listCollectionNames())
    collections.contains(collectionName)
  }

  override def createCollection(collectionName: String): Unit = ???

  override def dropCollection(collectionName: String): Unit = ???

  override def emptyCollection(collectionName: String): Unit = ???

  override def renameCollection(collectionNameOld: String, collectionNameNew: String): Unit = ???

  override def cloneCollection(collectionName: String, newCollectionName: String): Unit = ???

  override def insertDocument(collectionName: String, document: String): Unit = ???

  override def executeQuery(query: String): Unit = ???

  override def getDocuments(collectionName: String): Iterator[String] = ???

  private def synchronousCall[T](obs: Observable[T]): Seq[T] = {
    Await.result(obs.toFuture(), Duration(queryTimeout, TimeUnit.SECONDS))
  }

  private def synchronousCallFind[T](obs: FindObservable[T]): Seq[T] = {
    Await.result(obs.toFuture(), Duration(queryTimeout, TimeUnit.SECONDS))
  }

  private def synchronousCallSingle[T](obs: SingleObservable[T]): T = {
    Await.result(obs.toFuture(), Duration(queryTimeout, TimeUnit.SECONDS))
  }

  private def getCollection[T](collectionName: String)(implicit ct: ClassTag[T]): MongoCollection[T] = {
    db.getCollection[T](dbVersionCollectionName).withCodecRegistry(codecRegistry)
  }
}
