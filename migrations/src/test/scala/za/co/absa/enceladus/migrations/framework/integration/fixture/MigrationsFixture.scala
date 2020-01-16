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

package za.co.absa.enceladus.migrations.framework.integration.fixture

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.enceladus.migrations.framework.dao.{MongoDb, ScalaMongoImplicits}
import za.co.absa.enceladus.migrations.framework.integration.config.IntegrationTestConfiguration
import za.co.absa.enceladus.migrations.framework.migration.{ASC, IndexField}

trait MigrationsFixture extends BeforeAndAfterAll {

  this: Suite =>

  private val mongoConnectionString = IntegrationTestConfiguration.getMongoDbConnectionString
  private val integrationTestDbName = IntegrationTestConfiguration.getMongoDbDatabase

  protected var mongoClient: MongoClient = _
  protected var db: MongoDb = _
  protected var dbRaw: MongoDatabase = _

  import ScalaMongoImplicits._

  override protected def beforeAll(): Unit = {
    initDatabase()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally mongoClient.getDatabase(integrationTestDbName).drop().execute()
  }

  private def initDatabase(): Unit = {
    mongoClient = MongoClient(mongoConnectionString)
    val dbs = mongoClient.listDatabaseNames().execute()
    if (dbs.contains(integrationTestDbName)) {
      throw new IllegalStateException(s"MongoDB migration use case integration test database " +
      s"'$integrationTestDbName' already exists at '$mongoConnectionString'.")
    }
    dbRaw = mongoClient.getDatabase(integrationTestDbName)
    db = new MongoDb(dbRaw)
    fillInitialData()
  }

  private def fillInitialData(): Unit = {
    db.createCollection("foo1")
    db.createCollection("foo2")
    db.createCollection("foo3")
    db.insertDocument("foo1", """ { "name" : "Gizmo" } """)
    db.insertDocument("foo1", """ { "name" : "Doodad" } """)
    db.insertDocument("foo2", """ { "name" : "Dingus" } """)
    db.insertDocument("foo3", """ { "name" : "Doohickey" } """)
    db.createIndex("foo1", IndexField("date", ASC) :: Nil)
  }
}
