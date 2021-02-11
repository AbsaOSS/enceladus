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

package za.co.absa.enceladus.utils.validation.field

import org.apache.spark.sql.types.{BinaryType, MetadataBuilder, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationWarning}

class BinaryValidatorSuite extends AnyFunSuite {
  private implicit val defaults: Defaults = GlobalDefaults

  private def field(defaultValue: Option[String] = None, encoding: Option[String] = None, nullable: Boolean = true): TypedStructField = {
    val base = new MetadataBuilder()
    val builder2 = defaultValue.map(base.putString(MetadataKeys.DefaultValue, _)).getOrElse(base)
    val builder3 = encoding.map(builder2.putString(MetadataKeys.Encoding, _)).getOrElse(builder2)
    val result = StructField("test_field", BinaryType, nullable, builder3.build())
    TypedStructField(result)
  }

  test("field with no meta validates") {
    assert(BinaryFieldValidator.validate(field()).isEmpty)
  }

  test("field with explicit default or explicit non-base64 encoding validates") {
    assert(BinaryFieldValidator.validate(field(defaultValue = Some("abc"), encoding = Some("none"))).isEmpty)
    assert(BinaryFieldValidator.validate(field(encoding = Some("none"))).isEmpty)

    assert(BinaryFieldValidator.validate(field(defaultValue = Some("abc"))) == Seq(
      ValidationWarning("Default value of 'abc' found, but no encoding is specified. Assuming 'none'.")
    ))
  }

  test("field with base64 encoding and with no or correct defaultValue validates") {
    assert(BinaryFieldValidator.validate(field(encoding = Some("base64"))).isEmpty)
    // base64("test") => "dGVzdA=="
    assert(BinaryFieldValidator.validate(field(defaultValue = Some("dGVzdA=="), encoding = Some("base64"))).isEmpty)
  }

  test("field with base64 encoding and non-base64 default has issues ") {
    val result = BinaryFieldValidator.validate(field(defaultValue = Some("bogus!@#"), encoding = Some("base64")))
    assert(result.contains(ValidationError("'bogus!@#' cannot be cast to binary")))
    assert(result.contains(ValidationError("Invalid default value bogus!@# for Base64 encoding (cannot be decoded)!")))
  }

}
