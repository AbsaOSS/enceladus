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

package za.co.absa.enceladus.migrations.framework.fixture

import za.co.absa.enceladus.migrations.framework.migration._

object MigrationTestData {

  object MigrationExample0 extends MigrationBase with CollectionMigration {
    override val targetVersion: Int = 0

    createCollection("dataset")
    createCollection("schema")
    createCollection("mapping")
    createCollection("foo")
  }

  object MigrationExample1 extends MigrationBase with CollectionMigration with JsonMigration with CommandMigration {
    override val targetVersion: Int = 1

    renameCollection("mapping", "mapping_table")
    dropCollection("foo")
    createCollection("attachment")
    createIndex("dataset", IndexField("order", ASC) :: Nil)

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_transformed"
    })

    runCommand("schema") (versionedCollectionName => {
      s"$versionedCollectionName.execute(script1)"
    })

    runCommand("schema") (versionedCollectionName => {
      s"$versionedCollectionName.execute(script2)"
    })
  }

  object MigrationExample2 extends MigrationBase with CommandMigration {
    override val targetVersion: Int = 2

    runCommand("schema") (versionedCollectionName => {
      s"$versionedCollectionName.execute(script3)"
    })

    runCommand("schema") (versionedCollectionName => {
      s"$versionedCollectionName.execute(script4)"
    })
  }

  object MigrationExample3 extends MigrationBase with JsonMigration with CollectionMigration {
    override val targetVersion: Int = 3

    dropIndex("dataset", IndexField("order", ASC) :: Nil)

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("attachment")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3Throw extends MigrationBase with JsonMigration {
    override val targetVersion: Int = 3

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3WrongCollection extends MigrationBase with JsonMigration {
    override val targetVersion: Int = 3

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("foo")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3InconsistentManipulations extends MigrationBase with CollectionMigration{
    override val targetVersion: Int = 3

    renameCollection("attachment", "attachment2")
    dropCollection("attachment")
  }

}
