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

package za.co.absa.enceladus.rest_api.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.rest_api.models.rest.exceptions.SchemaFormatException

class SchemaTypeSuite extends AnyFlatSpec with Matchers {

  "SchemaType.fromSchemaName" should "correctly derive SchemaType.Value from string" in {
    SchemaType.fromSchemaName("struct") shouldBe SchemaType.Struct
    SchemaType.fromSchemaName("copybook") shouldBe SchemaType.Copybook
    SchemaType.fromSchemaName("avro") shouldBe SchemaType.Avro
  }

  it should "fail appropriately when invalid schemaName is supplied" in {
    val caughtException = the[SchemaFormatException] thrownBy {
      SchemaType.fromSchemaName("bad")
    }
    caughtException.schemaType shouldBe "bad"
    caughtException.message should include ("not a recognized schema format")
    caughtException.cause shouldBe a[NoSuchElementException]
  }

  "SchemaType.fromOptSchemaName" should "correctly derive SchemaType.Value from an optional string" in {
    // legacy cases
    SchemaType.fromOptSchemaName(None) shouldBe SchemaType.Struct
    SchemaType.fromOptSchemaName(Some("")) shouldBe SchemaType.Struct

    // regular cases
    SchemaType.fromOptSchemaName(Some("struct")) shouldBe SchemaType.Struct
    SchemaType.fromOptSchemaName(Some("copybook")) shouldBe SchemaType.Copybook
    SchemaType.fromOptSchemaName(Some("avro")) shouldBe SchemaType.Avro
  }

  it should "fail appropriately when invalid optional schemaName is supplied" in {
    val caughtException = the[SchemaFormatException] thrownBy {
      SchemaType.fromOptSchemaName(Some("worse"))
    }
    caughtException.schemaType shouldBe "worse"
    caughtException.message should include ("not a recognized schema format")
    caughtException.cause shouldBe a[NoSuchElementException]
  }

}
