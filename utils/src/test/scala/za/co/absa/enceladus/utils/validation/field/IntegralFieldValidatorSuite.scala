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

import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, MetadataBuilder, ShortType, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.numeric.Radix
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField.IntegralTypeStructField
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationWarning}

class IntegralFieldValidatorSuite extends AnyFunSuite {
  private implicit val defaults: Defaults = GlobalDefaults

  private def field(dataType: DataType, metadataBuilder: MetadataBuilder): IntegralTypeStructField[_] = {
    val result = StructField("test_field", dataType,  nullable = false, metadataBuilder.build())
    TypedStructField(result).asInstanceOf[IntegralTypeStructField[_]]
  }

  private def field(dataType: DataType, pattern: Option[String], radix: Option[Radix]): IntegralTypeStructField[_] = {
    val builder = new MetadataBuilder()
    val builder2 = pattern.map(builder.putString(MetadataKeys.Pattern, _)).getOrElse(builder)
    val builder3 = radix.map(r => builder2.putString(MetadataKeys.Radix, r.value.toString)).getOrElse(builder2)
    field(dataType, builder3)
  }

  test("No Radix nor Pattern defined") {
    val f = field(LongType, None, None)
    assert(IntegralFieldValidator.validate(f).isEmpty)
  }

  test("Radix and Pattern collide") {
    val f = field(IntegerType, Option("##0"), Option(Radix(3)))
    assert(IntegralFieldValidator.validate(f) == Seq(
      ValidationWarning("Both Radix and Pattern defined for field test_field, for Radix different from Radix(10) Pattern is ignored"))
    )
  }

  test("Radix defined as default, Pattern defined non-default") {
    val f = field(ByteType, Option("##0"), Option(Radix(10)))
    assert(IntegralFieldValidator.validate(f).isEmpty)
  }

  test("Radix defined is non-default, Pattern defined as default") {
    val f = field(ShortType, Option(""), Option(Radix(16)))
    assert(IntegralFieldValidator.validate(f).isEmpty)
  }

  test("Decimal symbols redefined wrongly, invalid pattern") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.GroupingSeparator, "Hello")
      .putLong(MetadataKeys.DecimalSeparator, 789)
      .putString(MetadataKeys.MinusSign, "")
      .putString(MetadataKeys.Pattern, "%0.###,#")
    val f = field(LongType, builder)
    val exp = Set(
      ValidationError(s"${MetadataKeys.GroupingSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.DecimalSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.MinusSign} metadata value of field 'test_field' is not Char in String format"),
      ValidationError("""Malformed pattern "%0.###,#"""")
    )
    assert(NumericFieldValidator.validate(f).toSet == exp)
  }

  test("Pattern defined, default value doesn't adhere to it") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.Pattern, "0XP")
      .putString(MetadataKeys.DefaultValue, "0.0XP")
    val f = field(IntegerType, builder)
    assert(NumericFieldValidator.validate(f) == Seq(
      ValidationError("Parsing of '0.0XP' failed.")
    ))
  }
}
