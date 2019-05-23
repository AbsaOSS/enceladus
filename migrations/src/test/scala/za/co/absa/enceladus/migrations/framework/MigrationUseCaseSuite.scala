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

  test("Test a simple migration") {
    val testData = new UseCaseTestData

    import testData._

    val mig = new Migrator(db, Migration0 :: Migration1 :: Nil)

    mig.migrate(1)

    assert(db.getQueriesExecuted.size == 2)
    assert(db.getQueriesExecuted == List("dataset_v1.execute(script1)", "mappingtable_v1.execute(script2)"))

    val expectedSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\",\"nullable\":" +
      "true,\"metadata\":{}},{\"name\":\"field2\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
      "{\"name\":\"field3\",\"type\":\"date\",\"nullable\":true,\"metadata\":{\"pattern\":\"yyyy-MM-dd\"}}," +
      "{\"name\":\"field4\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
    val actualSchema = db.getDocuments("schema_v1").next()
    assert(actualSchema == expectedSchema)

    assert(db.collectionExists("dataset_v1"))
    assert(db.collectionExists("schema_v1"))
    assert(db.collectionExists("mapping_table_v1"))
    assert(db.collectionExists("attachment_v1"))
    assert(db.collectionExists("foo"))
    assert(db.collectionExists("mappingtable"))
    assert(!db.collectionExists("foo_v1"))
    assert(!db.collectionExists("mappingtable_v1"))
  }

}
