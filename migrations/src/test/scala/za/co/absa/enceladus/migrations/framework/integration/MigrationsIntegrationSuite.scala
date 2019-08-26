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

import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.framework.Configuration.DatabaseVersionCollectionName
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.dao.ScalaMongoImplicits
import za.co.absa.enceladus.migrations.framework.integration.fixture.MigrationsFixture
import za.co.absa.enceladus.migrations.framework.integration.data.IntegrationTestData

class MigrationsIntegrationSuite extends FunSuite with MigrationsFixture {
  val testData = new IntegrationTestData

  import testData._
  import ScalaMongoImplicits._

  test("Test a migration") {
    val mig = new Migrator(db, Migration0 :: Migration1 :: Nil)

    assert(!db.doesCollectionExists(DatabaseVersionCollectionName))

    mig.migrate(1)

    assert(db.doesCollectionExists(DatabaseVersionCollectionName))
    assert(db.getVersion() == 1)

    assert(db.doesCollectionExists("foo1"))
    assert(db.doesCollectionExists("foo2"))
    assert(db.doesCollectionExists("foo3"))

    assert(db.doesCollectionExists("foo1_v1"))
    assert(!db.doesCollectionExists("foo2_v1"))
    assert(!db.doesCollectionExists("foo3_v1"))
    assert(db.doesCollectionExists("bar1_v1"))
    assert(db.doesCollectionExists("bar2_v1"))

    val docs = db.getDocuments("foo1_v1")

    val doc1 = docs.next()
    val doc2 = docs.next()

    assert(doc1.contains(""""name": "Gizmo", "date": "2018-10-10""""))
    assert(doc2.contains(""""name": "Hickey""""))
  }

  test("Test a database initialization") {
    val mig = new Migrator(db, Migration0 :: Migration1 :: Migration2 :: Nil)

    assert(db.doesCollectionExists(DatabaseVersionCollectionName))
    assert(!mig.isDatabaseEmpty())
    assert(mig.isMigrationRequired(2))

    db.dropCollection("foo1")
    db.dropCollection("foo2")
    db.dropCollection("foo3")
    db.dropCollection("foo1_v1")
    db.dropCollection("bar1_v1")
    db.dropCollection("bar2_v1")
    db.dropCollection(DatabaseVersionCollectionName)

    assert(mig.isDatabaseEmpty())

    mig.initializeDatabase(2)

    assert(db.doesCollectionExists(DatabaseVersionCollectionName))
    assert(db.getVersion() == 2)

    assert(!db.doesCollectionExists("foo1"))
    assert(!db.doesCollectionExists("foo2"))
    assert(!db.doesCollectionExists("foo3"))
    assert(!db.doesCollectionExists("foo1_v1"))
    assert(!db.doesCollectionExists("foo2_v1"))
    assert(!db.doesCollectionExists("foo3_v1"))
    assert(!db.doesCollectionExists("bar1_v1"))
    assert(!db.doesCollectionExists("bar2_v1"))

    assert(db.doesCollectionExists("foo1_v2"))
    assert(db.doesCollectionExists("bar1_v2"))
    assert(db.doesCollectionExists("bar2_v2"))

    val idx1 = dbRaw.getCollection("foo1_v2").listIndexes().execute()
    assert(idx1.size == 3)

    // Check if the second index contains 2 keys
    val indexDocs = idx1.toIndexedSeq.map(idx => idx("key").asDocument())
    assert(indexDocs(2).size == 2)
  }

}
