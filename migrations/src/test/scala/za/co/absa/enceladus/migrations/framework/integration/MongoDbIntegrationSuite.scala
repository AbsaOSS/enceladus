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
import za.co.absa.enceladus.migrations.framework.integration.fixture.MongoDbFixture

class MongoDbIntegrationSuite extends FunSuite with MongoDbFixture {

  test("Test add/drop collections") {
    assert(!db.isCollectionExists("foo"))

    db.createCollection("foo")
    assert(db.isCollectionExists("foo"))

    db.dropCollection("foo")
    assert(!db.isCollectionExists("foo"))
  }

  test("Test that the initial database version is 0") {
    assert(db.getVersion() == 0)
  }

  test("Test setting version works") {
    db.setVersion(1)
    assert(db.getVersion() == 1)
  }

  test("Test db cloning works") {
    db.createCollection("bar1")
    db.insertDocument("bar1", "{\"item\": \"1\"}")
    db.cloneCollection("bar1", "bar2")
    val doc1 = db.getDocuments("bar1").next()
    val doc2 = db.getDocuments("bar2").next()

    assert(doc2.contains(""""item" : "1""""))
    assert(doc1 == doc2)
    assert(db.isCollectionExists("bar1"))
    assert(db.isCollectionExists("bar2"))

    db.dropCollection("bar1")
    db.dropCollection("bar2")
  }

  test("Test empty collection works") {
    db.createCollection("bar3")
    db.insertDocument("bar3", "{\"item\": \"1\"}")
    db.emptyCollection("bar3")

    val docs = db.getDocuments("bar3")

    assert(!docs.hasNext)

    db.dropCollection("bar3")
  }

  test("Test collection renaming works") {
    db.createCollection("bar4")
    db.renameCollection("bar4", "bar5")

    assert(db.isCollectionExists("bar5"))
    assert(!db.isCollectionExists("bar4"))

    db.dropCollection("bar5")
  }
}
