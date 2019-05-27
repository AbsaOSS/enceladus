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

package za.co.absa.enceladus.migrations.framework.integration

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.framework.dao.MongoDb

class MongoDbIntegrationSuite extends FunSuite {

  val integrationTestDbName = "migrations_integration"
  val mongoConnectionString = "mongodb://localhost:27017"

  val mongoClient: MongoClient = MongoClient(mongoConnectionString)
  val db: MongoDb = initDatabase()

  test ("The initial database version should be 0") {
    assert(db.getVersion() == 0)
  }

  private def initDatabase(): MongoDb = {
    mongoClient.getDatabase(integrationTestDbName).drop()
    new MongoDb(mongoClient.getDatabase(integrationTestDbName))
  }

}
