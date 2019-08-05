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

package za.co.absa.enceladus.migrations.framework.continuous.integration

import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.continuous.migrate01.ContinuousMigrator
import za.co.absa.enceladus.migrations.framework.continuous.integration.fixture.ExampleDatabaseFixture

class ContinuousMigrationIntegrationSuite extends FunSuite with ExampleDatabaseFixture {

  test("Test schema migrates properly and conflicts are resolved") {
    val mig = new ContinuousMigrator(db, db)

    mig.migrate()

    // Schemas
    // Original
    assert(schemaExists("TestSchema1", 1) )
    assert(schemaExists("TestSchema2", 1) )
    assert(schemaExists("TestSchema2", 2) )
    assert(schemaExists("TestSchema2", 3) )
    assert(schemaExists("TestSchema3", 1) )
    assert(schemaExists("TestSchema4", 1) )

    // Migrated
    assert(schemaExists("TestSchema1", 2) )
    assert(schemaExists("TestSchema2", 4) )
    assert(schemaExists("TestSchema4", 2) )

    // Mapping tables
    // Original
    assert(mappingTableExists("TestMT1", 1, "TestSchema1", 1) )
    assert(mappingTableExists("TestMT2", 1, "TestSchema2", 1) )
    assert(mappingTableExists("TestMT2", 2, "TestSchema2", 2) )
    assert(mappingTableExists("TestMT2", 3, "TestSchema2", 3) )
    assert(mappingTableExists("TestMT3", 1, "TestSchema3", 1) )
    assert(mappingTableExists("TestMT4", 1, "TestSchema4", 1) )

    // Migrated
    assert(mappingTableExists("TestMT1", 2, "TestSchema1", 2) )
    assert(mappingTableExists("TestMT2", 4, "TestSchema2", 4) )
    assert(mappingTableExists("TestMT4", 2, "TestSchema4", 2) )
    assert(!mappingTableExists("TestMT4", 2, "TestSchema4", 1) )
  }

}
