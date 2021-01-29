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

package za.co.absa.enceladus.migrations.framework

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.migrations.framework.fixture.MigrationTestData._
import za.co.absa.enceladus.migrations.framework.fixture.MigrationTestDoubles._

class BaseMigrationSuite extends AnyFunSuite {

  test("Test collection names are determined properly for a given db version") {
    val mig = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample1 :: Nil)

    val collectionsInV0 = mig.getCollectionNames(0)
    val collectionsInV1 = mig.getCollectionNames(1)

    assert(collectionsInV0 == "dataset" :: "schema" :: "mapping" :: "foo" :: Nil)
    assert(collectionsInV1 == "dataset" :: "schema" :: "mapping_table" :: "attachment" :: Nil)
  }

  test("Test a migration provides JSON transformations and queries") {
    assert(MigrationExample1.getTransformer("schema").isEmpty)
    assert(MigrationExample1.getCommands("dataset") == Nil)

    val transformation = MigrationExample1.getTransformer("dataset")
    assert(transformation.nonEmpty)
    assert (transformation.get.apply("foo") == "foo_transformed")

    val queries = MigrationExample1.getCommands("schema")
    assert(queries.length == 2)
    assert(queries.head == "schema_v1.execute(script1)")
    assert(queries(1) == "schema_v1.execute(script2)")
  }

  test("Test that migrator checks for invalid transformations") {
    intercept[ExceptionInInitializerError] {
      new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample2 :: MigrationExample3Throw :: Nil)
    }
  }

  test("Test that an exception is thrown if migration versions are inconsistent") {
    val migGap = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample2 :: Nil)
    val migOutOfOrder = new Migrator(DocumentDbStub, MigrationExample1 :: MigrationExample0 :: Nil)
    val migUnreachable = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample1 :: Nil)

    intercept[IllegalStateException] {
      migGap.validate(2)
    }

    intercept[IllegalStateException] {
      migOutOfOrder.validate(1)
    }

    intercept[IllegalStateException] {
      migUnreachable.validate(2)
    }
  }

  test("Test that an exception is thrown if a migration requires a collection that does not exist") {
    val mig = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample1 :: MigrationExample2 ::
      MigrationExample3WrongCollection :: Nil)
    intercept[IllegalStateException] {
      mig.validate(3)
    }
  }

  test("Test that an exception is thrown if collections are not manipulated consistently") {
    val mig = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample1 :: MigrationExample2 ::
      MigrationExample3InconsistentManipulations :: Nil)
    intercept[IllegalStateException] {
      mig.validate(3)
    }
  }

  test("Test that an exception is thrown if the target database version is not reachable") {
    val mig = new Migrator(DocumentDbStub, MigrationExample0 :: MigrationExample1 :: MigrationExample2 :: Nil)
    intercept[IllegalStateException] {
      mig.validate(3)
    }
  }

}
