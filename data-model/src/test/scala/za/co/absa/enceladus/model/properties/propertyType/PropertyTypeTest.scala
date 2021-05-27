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

package za.co.absa.enceladus.model.properties.propertyType

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PropertyTypeTest extends AnyFlatSpec with Matchers {

  "EnumPropertyType" should "check suggestedValue conformity with defined allowedValues" in {
    val errorMessage = intercept[PropertyTypeValidationException] {
      EnumPropertyType(Seq("optionA", "optionB", "optionC"), suggestedValue = Some("invalidOption"))
    }.getMessage
    errorMessage should include("Value 'invalidOption' is not one of the allowed values (optionA, optionB, optionC).")
  }

  it should "check allowedValues to contain at least 1 item" in {
    val errorMessage = intercept[IllegalArgumentException] {
      EnumPropertyType(Seq.empty, suggestedValue = None)
    }.getMessage
    errorMessage should include("At least one allowed value must be defined for EnumPropertyType.")
  }

  it should "check allowedValues only to contain nonempty strings" in {
    Seq(
      Seq(""),
      Seq("", "other", "values")
    ).foreach { invalidValues =>
      intercept[IllegalArgumentException] {
        EnumPropertyType(invalidValues, suggestedValue = None)
      }.getMessage should include("Empty string is disallowed as an allowedValue for EnumPropertyType.")
    }
  }

  it should "check allowedValues contains unique values" in {
    val nonUniqueAllowedValues = Seq("duplicate", "duplicate", "other")
      intercept[IllegalArgumentException] {
        EnumPropertyType(nonUniqueAllowedValues, suggestedValue = None)
      }.getMessage should include("Allowed values for EnumPropertyType should be unique")
  }

  it should "allow empty suggested value no matter the allowedValues" in {
    EnumPropertyType(Seq("optionA", "optionB", "optionC"), suggestedValue = None) // throws no exception
  }


}
