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

package za.co.absa.enceladus.migrations.framework

import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.framework.fixture.UseCaseTestData

class MigrationUseCaseSuite extends FunSuite {

  test("Test a database initialization") {
    val testData = new UseCaseTestData

    import testData._

    val db = emptyDb

    val mig = new Migrator(db, Migration0 :: Migration1 :: Migration2 :: Nil)

    mig.initializeDatabase(2)

    assert(db.getVersion() == 2)

    val expectedActions =
        "create(dataset_v2)" ::
        "create(schema_v2)" ::
        "create(mapping_table_v2)" ::
        "create(attachment_v2)" ::
        "createIndex(dataset_v2,List((name: ASC)),true)" ::
        "createIndex(dataset_v2,List((version: DESC)),false)" ::
        "setDbVersion(2)" ::
        Nil

    assert(db.getActionsExecuted == expectedActions)

    assert(db.doesCollectionExists("dataset_v2"))
    assert(db.doesCollectionExists("schema_v2"))
    assert(db.doesCollectionExists("mapping_table_v2"))
    assert(db.doesCollectionExists("attachment_v2"))
    assert(!db.doesCollectionExists("dataset_v1"))
    assert(!db.doesCollectionExists("schema_v1"))
    assert(!db.doesCollectionExists("mapping_table_v1"))
    assert(!db.doesCollectionExists("attachment_v1"))
    assert(!db.doesCollectionExists("foo"))
    assert(!db.doesCollectionExists("mappingtable"))
    assert(!db.doesCollectionExists("foo_v1"))
    assert(!db.doesCollectionExists("mappingtable_v1"))
  }

  test("Test a simple migration") {
    val testData = new UseCaseTestData

    import testData._

    val mig = new Migrator(db, Migration0 :: Migration1 :: Nil)

    mig.migrate(1)

    assert(db.getVersion() == 1)

    val expectedActions =
        "clone(dataset,dataset_v1)" ::
        "clone(schema,schema_v1)" ::
        "clone(mappingtable,mappingtable_v1)" ::
        "clone(foo,foo_v1)" ::
        "create(attachment_v1)" ::
        "drop(foo_v1)" ::
        "rename(mappingtable_v1,mapping_table_v1)" ::
        "createIndex(dataset_v1,List((order: ASC)),false)" ::
        "createIndex(dataset_v1,List((version: DESC)),false)" ::
        "empty(schema_v1)" ::
        "forEachDocument(schema)" ::
        "insertTo(schema_v1)" ::
        "{collStats: \"dataset_v1\", scale: undefined, $readPreference: { mode: \"secondaryPreferred\"}}" ::
        "{collStats: \"mappingtable_v1\", $readPreference: { mode: \"secondaryPreferred\"}}" ::
        "setDbVersion(1)" ::
        Nil
    assert(db.getActionsExecuted == expectedActions)

    val expectedSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\",\"nullable\":" +
      "true,\"metadata\":{}},{\"name\":\"field2\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
      "{\"name\":\"field3\",\"type\":\"date\",\"nullable\":true,\"metadata\":{\"pattern\":\"yyyy-MM-dd\"}}," +
      "{\"name\":\"field4\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
    val actualSchema = db.getDocuments("schema_v1").next()
    assert(actualSchema == expectedSchema)

    assert(db.doesCollectionExists("dataset_v1"))
    assert(db.doesCollectionExists("schema_v1"))
    assert(db.doesCollectionExists("mapping_table_v1"))
    assert(db.doesCollectionExists("attachment_v1"))
    assert(db.doesCollectionExists("foo"))
    assert(db.doesCollectionExists("mappingtable"))
    assert(!db.doesCollectionExists("foo_v1"))
    assert(!db.doesCollectionExists("mappingtable_v1"))
  }

  test("Test a multistep migration") {
    val testData = new UseCaseTestData

    import testData._

    val mig = new Migrator(db, Migration0 :: Migration1 :: Migration2 :: Nil)

    mig.migrate(2)

    assert(db.getVersion() == 2)

    val expectedActions =
        "clone(dataset,dataset_v1)" ::
        "clone(schema,schema_v1)" ::
        "clone(mappingtable,mappingtable_v1)" ::
        "clone(foo,foo_v1)" ::
        "create(attachment_v1)" ::
        "drop(foo_v1)" ::
        "rename(mappingtable_v1,mapping_table_v1)" ::
        "createIndex(dataset_v1,List((order: ASC)),false)" ::
        "createIndex(dataset_v1,List((version: DESC)),false)" ::
        "empty(schema_v1)" ::
        "forEachDocument(schema)" ::
        "insertTo(schema_v1)" ::
        "{collStats: \"dataset_v1\", scale: undefined, $readPreference: { mode: \"secondaryPreferred\"}}" ::
        "{collStats: \"mappingtable_v1\", $readPreference: { mode: \"secondaryPreferred\"}}" ::
        "setDbVersion(1)" ::
        "drop(schema_v2)" ::
        "clone(dataset_v1,dataset_v2)" ::
        "clone(schema_v1,schema_v2)" ::
        "clone(mapping_table_v1,mapping_table_v2)" ::
        "clone(attachment_v1,attachment_v2)" ::
        "dropIndex(dataset_v2,List((order: ASC)))" ::
        "empty(schema_v2)" ::
        "forEachDocument(schema_v1)" ::
        "insertTo(schema_v2)" ::
        "{collStats: \"schema_v2\"}" ::
        "setDbVersion(2)" ::
        Nil

    assert(db.getActionsExecuted == expectedActions)

    val expectedSchema = "{\"new_schema\": \"none\"}"
    val actualSchema = db.getDocuments("schema_v2").next()
    assert(actualSchema == expectedSchema)

    assert(db.doesCollectionExists("dataset_v2"))
    assert(db.doesCollectionExists("schema_v2"))
    assert(db.doesCollectionExists("mapping_table_v2"))
    assert(db.doesCollectionExists("attachment_v2"))
    assert(db.doesCollectionExists("dataset_v1"))
    assert(db.doesCollectionExists("schema_v1"))
    assert(db.doesCollectionExists("mapping_table_v1"))
    assert(db.doesCollectionExists("attachment_v1"))
    assert(db.doesCollectionExists("foo"))
    assert(db.doesCollectionExists("mappingtable"))
    assert(!db.doesCollectionExists("foo_v1"))
    assert(!db.doesCollectionExists("mappingtable_v1"))
  }

  test("Test a migration that tries to add a collection that already exists") {
    val testData = new UseCaseTestData

    import testData._

    val mig = new Migrator(db, Migration0 :: Migration1 :: Migration2 :: Migration3err :: Nil)

    assert(intercept[IllegalStateException] {
      mig.migrate(3)
    }.getMessage contains "Attempt to add a collection that already exists")

  }

}
