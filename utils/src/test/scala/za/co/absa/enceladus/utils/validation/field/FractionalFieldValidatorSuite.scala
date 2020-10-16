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

import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, MetadataBuilder, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField.FractionalTypeStructField
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}
import za.co.absa.enceladus.utils.validation.ValidationError

class FractionalFieldValidatorSuite extends AnyFunSuite {
  private implicit val defaults: Defaults = GlobalDefaults

  private def field(dataType: DataType, metadataBuilder: MetadataBuilder): FractionalTypeStructField[_] = {
    val result = StructField("test_field", dataType,  nullable = false, metadataBuilder.build())
    TypedStructField(result).asInstanceOf[FractionalTypeStructField[_]]
  }

  test("No allow_infinity metadata") {
    val builder = new MetadataBuilder
    val f = field(FloatType, builder)
    assert(FractionalFieldValidator.validate(f).isEmpty)
  }

  test("allow_infinity metadata defined") {
    val builder1 = new MetadataBuilder().putString(MetadataKeys.AllowInfinity, "false")
    val f1 = field(FloatType, builder1)
    assert(FractionalFieldValidator.validate(f1).isEmpty)
    val builder2 = new MetadataBuilder().putString(MetadataKeys.AllowInfinity, "True")
    val f2 = field(DoubleType, builder2)
    assert(FractionalFieldValidator.validate(f2).isEmpty)
  }

  test("allow_infinity not boolean") {
    val builder = new MetadataBuilder().putString(MetadataKeys.AllowInfinity, "23")
    val f = field(FloatType, builder)
    assert(FractionalFieldValidator.validate(f) == Seq(
      ValidationError(s"${MetadataKeys.AllowInfinity} metadata value of field 'test_field' is not Boolean in String format")
    ))
  }

  test("allow_infinity boolean in binary form") {
    val builder = new MetadataBuilder().putBoolean(MetadataKeys.AllowInfinity, value = true)
    val f = field(FloatType, builder)
    assert(FractionalFieldValidator.validate(f) == Seq(
      ValidationError(s"${MetadataKeys.AllowInfinity} metadata value of field 'test_field' is not Boolean in String format")
    ))
  }

  test("Decimal symbols redefined wrongly, invalid pattern") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.GroupingSeparator, "")
      .putString(MetadataKeys.DecimalSeparator, "xxx")
      .putLong(MetadataKeys.MinusSign, 1)
      .putString(MetadataKeys.Pattern, "0.###,#")
    val f = field(DoubleType, builder)
    val exp = Set(
      ValidationError(s"${MetadataKeys.GroupingSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.DecimalSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.MinusSign} metadata value of field 'test_field' is not Char in String format"),
      ValidationError("""Malformed pattern "0.###,#"""")
    )
    assert(NumericFieldValidator.validate(f).toSet == exp)
  }

  test("Pattern defined, default value doesn't adhere to it") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.Pattern, "0.#MPH")
      .putString(MetadataKeys.DefaultValue, "0.0")
    val f = field(FloatType, builder)
    assert(NumericFieldValidator.validate(f) == Seq(
      ValidationError("Parsing of '0.0' failed.")
    ))
  }

}
