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

import za.co.absa.enceladus.migrations.framework.migration.{CollectionMigration, JsonMigration, QueryMigration}

object MigrationTestData {

  object MigrationExample0 extends CollectionMigration {
    override val targetVersion: Int = 0

    addCollection("dataset")
    addCollection("schema")
    addCollection("mapping")
    addCollection("foo")
  }

  object MigrationExample1 extends JsonMigration with QueryMigration with CollectionMigration {
    override val targetVersion: Int = 1

    renameCollection("mapping", "mapping_table")
    removeCollection("foo")
    addCollection("attachment")

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_transformed"
    })

    applyQuery("schema") {
      "script1"
    }

    applyQuery("schema") {
      "script2"
    }
  }

  object MigrationExample2 extends QueryMigration {
    override val targetVersion: Int = 2

    applyQuery("schema") {
      "script3"
    }

    applyQuery("schema") {
      "script4"
    }
  }

  object MigrationExample3 extends JsonMigration {
    override val targetVersion: Int = 3

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("attachment")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3Throw extends JsonMigration {
    override val targetVersion: Int = 3

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3WrongCollection extends JsonMigration {
    override val targetVersion: Int = 3

    transformJSON("dataset")(jsonIn => {
      jsonIn + "_t1"
    })

    transformJSON("foo")(jsonIn => {
      jsonIn + "_t2"
    })
  }

  object MigrationExample3InconsistentManipulations extends CollectionMigration {
    override val targetVersion: Int = 3

    renameCollection("attachment", "attachment2")
    removeCollection("attachment")
  }

}
