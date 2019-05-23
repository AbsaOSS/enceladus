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

  initializeDb()

  object Migration0 extends MigrationBase with CollectionMigration {
    override val targetVersion: Int = 0

    addCollection("dataset")
    addCollection("schema")
    addCollection("mappingtable")
    addCollection("foo")
  }

  object Migration1 extends MigrationBase with CollectionMigration with JsonMigration with QueryMigration  {
    override val targetVersion: Int = 1

    renameCollection("mappingtable", "mapping_table")
    removeCollection("foo")
    addCollection("attachment")

    transformJSON("schema")(jsonIn => {
      jsonIn.replace(" ","")
        .replace("\n", "")
        .replace("\t","")
    })

    applyQuery("dataset") ( versionedCollectionName => {
      s"$versionedCollectionName.execute(script1)"
    })

    applyQuery("mappingtable") ( versionedCollectionName => {
      s"$versionedCollectionName.execute(script2)"
    })
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
  }

}
