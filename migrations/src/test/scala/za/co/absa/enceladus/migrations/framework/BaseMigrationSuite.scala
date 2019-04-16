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
import za.co.absa.enceladus.migrations.framework.migration.{JsonMigration, QueryMigration}

class BaseMigrationSuite extends FunSuite {

  // This is an example migration spec
  object MigrationExample1 extends JsonMigration with QueryMigration {
    override val targetVersion: Int = 1

    migrate("dataset")(jsonIn => {
      jsonIn + "_transformed"
    })

    applyQuery("schema") {
      "script1"
    }

    applyQuery("schema") {
      "script2"
    }
  }


  test("Test a migration provides JSON transformations and queries") {
    assert(MigrationExample1.getTransformer("schema").isEmpty)
    assert(MigrationExample1.getQueries("dataset") == Nil)

    val transformation = MigrationExample1.getTransformer("dataset")
    assert(transformation.nonEmpty)
    assert (transformation.get.apply("foo") == "foo_transformed")

    val queries = MigrationExample1.getQueries("schema")
    assert(queries.length == 2)
    assert(queries.head == "script1")
    assert(queries(1) == "script2")
  }

}
