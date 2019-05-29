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
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.integration.fixture.MigrationsFixture
import za.co.absa.enceladus.migrations.framework.integration.data.IntegrationTestData

class MigrationsIntegrationSuite extends FunSuite with MigrationsFixture {
  val testData = new IntegrationTestData

  import testData._

  test("Test a migration") {
    val mig = new Migrator(db, Migration0 :: Migration1 :: Nil)

    assert(!db.isCollectionExists("db_version"))

    mig.migrate(1)

    assert(db.isCollectionExists("db_version"))
    assert(db.getVersion() == 1)

    assert(db.isCollectionExists("foo1"))
    assert(db.isCollectionExists("foo2"))
    assert(db.isCollectionExists("foo3"))

    assert(db.isCollectionExists("foo1_v1"))
    assert(!db.isCollectionExists("foo2_v1"))
    assert(!db.isCollectionExists("foo3_v1"))
    assert(db.isCollectionExists("bar1_v1"))
    assert(db.isCollectionExists("bar2_v1"))

    val docs = db.getDocuments("foo1_v1")

    val doc1 = docs.next()
    val doc2 = docs.next()

    assert(doc1.contains(""""name" : "Gizmo", "date" : "2018-10-10""""))
    assert(doc2.contains(""""name" : "Hickey""""))
  }

}
