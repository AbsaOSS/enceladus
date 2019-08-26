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

import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.framework.integration.fixture.MongoDbFixture
import za.co.absa.enceladus.migrations.framework.migration.{ASC, DESC, IndexField}

class MongoDbIntegrationSuite extends FunSuite with MongoDbFixture {
  import za.co.absa.enceladus.migrations.framework.dao.ScalaMongoImplicits._

  test("Test add/drop collections") {
    assert(!db.doesCollectionExists("foo"))

    db.createCollection("foo")
    assert(db.doesCollectionExists("foo"))

    db.dropCollection("foo")
    assert(!db.doesCollectionExists("foo"))
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

    assert(doc2.contains(""""item": "1""""))
    assert(doc1 == doc2)
    assert(db.doesCollectionExists("bar1"))
    assert(db.doesCollectionExists("bar2"))

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

    assert(db.doesCollectionExists("bar5"))
    assert(!db.doesCollectionExists("bar4"))

    db.dropCollection("bar5")
  }

  test("Test index adding/removal") {
    db.createCollection("bar6")
    db.insertDocument("bar6", "{\"item\": \"1\"}")
    db.insertDocument("bar6", "{\"item\": \"2\"}")
    db.insertDocument("bar6", "{\"item\": \"3\"}")
    db.createIndex("bar6", IndexField("item", ASC) :: Nil)

    val idx1 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx1.size == 2)
    assert(idx1.contains(Document(
      """{
        |  "v": 2,
        |  "key": {
        |    "item": 1
        |  },
        |  "name": "item_1",
        |  "ns": "migrations_integration.bar6"
        |}""".stripMargin)))

    db.dropIndex("bar6", IndexField("item", ASC) :: Nil)
    val idx2 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx2.size == 1)

    db.dropCollection("bar6")
  }

  test("Test unique index adding/removal") {
    db.createCollection("bar6")
    db.insertDocument("bar6", "{\"item\": \"1\"}")
    db.insertDocument("bar6", "{\"item\": \"2\"}")
    db.insertDocument("bar6", "{\"item\": \"3\"}")
    db.createIndex("bar6", IndexField("item", ASC) :: Nil, unique = true)

    val idx1 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx1.size == 2)
    assert(idx1.contains(Document(
      """{
        |  "v": 2,
        |  "unique": true,
        |  "key": {
        |    "item": 1
        |  },
        |  "name": "item_1",
        |  "ns": "migrations_integration.bar6"
        |}""".stripMargin)))

    db.dropIndex("bar6", IndexField("item", ASC) :: Nil)
    val idx2 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx2.size == 1)

    db.dropCollection("bar6")
  }

  test("Test dropping an index with the wrong sort order") {
    db.createCollection("bar6")
    db.insertDocument("bar6", "{\"item\": \"1\"}")
    db.insertDocument("bar6", "{\"item\": \"2\"}")
    db.insertDocument("bar6", "{\"item\": \"3\"}")
    db.createIndex("bar6", IndexField("item", ASC) :: Nil)

    val idx1 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx1.size == 2)
    assert(idx1.contains(Document(
      """{
        |  "v": 2,
        |  "key": {
        |    "item": 1
        |  },
        |  "name": "item_1",
        |  "ns": "migrations_integration.bar6"
        |}""".stripMargin)))

    db.dropIndex("bar6", IndexField("item", DESC) :: Nil)
    val idx2 = dbRaw.getCollection("bar6").listIndexes().execute()
    assert(idx2.size == 2)
    assert(idx2.contains(Document(
      """{
        |  "v": 2,
        |  "key": {
        |    "item": 1
        |  },
        |  "name": "item_1",
        |  "ns": "migrations_integration.bar6"
        |}""".stripMargin)))

    db.dropCollection("bar6")
  }

  test("Test index copying on clone") {
    db.createCollection("bar7")
    db.insertDocument("bar7", "{\"name\": \"Apple\", \"type\": \"Fruit\"}")
    db.insertDocument("bar7", "{\"name\": \"Pear\", \"type\": \"Fruit\"}")
    db.insertDocument("bar7", "{\"name\": \"Orange\", \"type\": \"Fruit\"}")
    db.createIndex("bar7", IndexField("name", ASC) :: IndexField("type", DESC) :: Nil)

    val idx1 = dbRaw.getCollection("bar7").listIndexes().execute()
    assert(idx1.size == 2)
    assert(idx1.contains(Document(
      """{
        |  "v": 2,
        |  "key": {
        |    "name": 1,
        |    "type": -1
        |  },
        |  "name": "name_1_type_-1",
        |  "ns": "migrations_integration.bar7"
        |}""".stripMargin)))

    db.cloneCollection("bar7", "bar8")

    val idx2 = dbRaw.getCollection("bar8").listIndexes().execute()
    assert(idx2.size == 2)
    assert(idx2.contains(Document(
      """{
        |  "v": 2,
        |  "key": {
        |    "name": 1,
        |    "type": -1
        |  },
        |  "name": "name_1_type_-1",
        |  "ns": "migrations_integration.bar8"
        |}""".stripMargin)))

    db.dropCollection("bar7")
    db.dropCollection("bar8")
  }

  test("Test unique index copying on clone") {
    db.createCollection("bar7")
    db.insertDocument("bar7", "{\"name\": \"Apple\", \"type\": \"Fruit\"}")
    db.insertDocument("bar7", "{\"name\": \"Pear\", \"type\": \"Fruit\"}")
    db.insertDocument("bar7", "{\"name\": \"Orange\", \"type\": \"Fruit\"}")
    db.createIndex("bar7", IndexField("name", ASC) :: IndexField("type", ASC) :: Nil, unique = true)

    val idx1 = dbRaw.getCollection("bar7").listIndexes().execute()
    assert(idx1.size == 2)
    assert(idx1.contains(Document(
      """{
        |  "v": 2,
        |  "unique": true,
        |  "key": {
        |    "name": 1,
        |    "type": 1
        |  },
        |  "name": "name_1_type_1",
        |  "ns": "migrations_integration.bar7"
        |}""".stripMargin)))

    db.cloneCollection("bar7", "bar8")

    val idx2 = dbRaw.getCollection("bar8").listIndexes().execute()
    assert(idx2.size == 2)
    assert(idx2.contains(Document(
      """{
        |  "v": 2,
        |  "unique": true,
        |  "key": {
        |    "name": 1,
        |    "type": 1
        |  },
        |  "name": "name_1_type_1",
        |  "ns": "migrations_integration.bar8"
        |}""".stripMargin)))

    db.dropCollection("bar7")
    db.dropCollection("bar8")
  }

}
