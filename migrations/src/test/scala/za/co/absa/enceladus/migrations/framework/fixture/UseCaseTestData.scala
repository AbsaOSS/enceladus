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

package za.co.absa.enceladus.migrations.framework.fixture

import za.co.absa.enceladus.migrations.framework.migration._

/** This is a mocked database and migrations data for migration use case tests */
class UseCaseTestData {

  val db = new DocumentDbMock
  val emptyDb = new DocumentDbMock

  initializeDb()

  object Migration0 extends MigrationBase with CollectionMigration {
    override val targetVersion: Int = 0

    createCollection("dataset")
    createCollection("schema")
    createCollection("mappingtable")
    createCollection("foo")
    createIndex("dataset", IndexField("name", ASC) :: Nil)
  }

  object Migration1 extends MigrationBase with CollectionMigration with JsonMigration with CommandMigration  {
    override val targetVersion: Int = 1

    renameCollection("mappingtable", "mapping_table")
    dropCollection("foo")
    createCollection("attachment")
    createIndex("dataset", IndexField("order", ASC) :: Nil)
    createIndex("dataset", IndexField("version", ASC) :: Nil)

    transformJSON("schema")(jsonIn => {
      jsonIn.replace(" ","")
        .replace("\n", "")
        .replace("\r", "")
        .replace("\t","")
    })

    runCommand("dataset") (versionedCollectionName => {
      s"{collStats: ${'"'}$versionedCollectionName${'"'}, scale: undefined, $$readPreference: { " +
        s"mode: ${'"'}secondaryPreferred${'"'}}}"
    })

    runCommand("mappingtable") (versionedCollectionName => {
      s"{collStats: ${'"'}$versionedCollectionName${'"'}, $$readPreference: { mode: ${'"'}secondaryPreferred${'"'}}}"
    })
  }

  object Migration2 extends MigrationBase with CollectionMigration with JsonMigration with CommandMigration  {
    override val targetVersion: Int = 2

    dropIndex("dataset", IndexField("order", ASC) :: Nil)

    transformJSON("schema")(jsonIn => {
      "{\"new_schema\": \"none\"}"
    })

    runCommand("schema") (versionedCollectionName => {
      s"{collStats: ${'"'}$versionedCollectionName${'"'}}"
    })
  }

  object Migration3err extends MigrationBase with CollectionMigration  {
    override val targetVersion: Int = 3

    createCollection("attachment")
  }

  private def initializeDb(): Unit = {
    db.createCollection("schema")
    db.createCollection("dataset")
    db.createCollection("mappingtable")
    db.createCollection("foo")

    db.insertDocument("schema",
      """{
        |	"type": "struct",
        |	"fields": [
        |		{
        |			"name": "field1",
        |			"type": "string",
        |			"nullable": true,
        |			"metadata": {}
        |		},
        |		{
        |			"name": "field2",
        |			"type": "integer",
        |			"nullable": true,
        |			"metadata": {}
        |		},
        |		{
        |			"name": "field3",
        |			"type": "date",
        |			"nullable": true,
        |			"metadata": {
        |				"pattern": "yyyy-MM-dd"
        |			}
        |		},
        |		{
        |			"name": "field4",
        |			"type": "string",
        |			"nullable": true,
        |			"metadata": {}
        |		}
        |    ]
        |}""".stripMargin)

    // Insert some partially migrated data
    db.createCollection("schema_v2")
    db.insertDocument("schema_v2", "{bogus}")

    db.resetExecutedActions()
  }

}
