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

import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}
import za.co.absa.enceladus.utils.validation.ValidationError

class ScalarFieldValidatorSuite extends FunSuite {

  private implicit val defaults: Defaults = GlobalDefaults

  test("Default value is set") {
    val field = StructField("test_field", StringType, nullable = false, new MetadataBuilder().putString(MetadataKeys.DefaultValue, "foo").build())
    val testResult = ScalarFieldValidator.validate(TypedStructField(field))
    assert(testResult.isEmpty)
  }

  test("Default value is not set") {
    val field = StructField("test_field", StringType, nullable = true)
    val testResult = ScalarFieldValidator.validate(TypedStructField(field))
    assert(testResult.isEmpty)
  }

  test("Default value is set to NULL") {
    val field = StructField("test_field", StringType, nullable = true, new MetadataBuilder().putString(MetadataKeys.DefaultValue, null).build())
    val testResult = ScalarFieldValidator.validate(TypedStructField(field))
    assert(testResult.isEmpty)
  }

  test("Default value is set to non string value fails") {
    val field = StructField("test_field", StringType, nullable = false, new MetadataBuilder().putBoolean(MetadataKeys.DefaultValue, value = true).build())
    val testResult = ScalarFieldValidator.validate(TypedStructField(field))
    assert(testResult == Seq(ValidationError("java.lang.Boolean cannot be cast to java.lang.String")))
  }
}
