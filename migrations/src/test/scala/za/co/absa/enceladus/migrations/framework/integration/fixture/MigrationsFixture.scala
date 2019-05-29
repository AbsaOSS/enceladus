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

package za.co.absa.enceladus.migrations.framework.integration.fixture

import org.mongodb.scala.MongoClient
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.enceladus.migrations.framework.dao.{MongoDb, ScalaMongoImplicits}

trait MigrationsFixture extends BeforeAndAfterAll {

  this: Suite =>

  val integrationTestDbName = "migrations_integration2"
  val mongoConnectionString = "mongodb://localhost:27017"

  protected var mongoClient: MongoClient = _
  protected var db: MongoDb = _

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
    db = new MongoDb(mongoClient.getDatabase(integrationTestDbName))
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
  }
}
