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

package za.co.absa.enceladus.model

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality.Mandatory
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, PropertyTypeValidationException, StringPropertyType}

class PropertyDefinitionTest extends AnyFunSuite {

  private val modelVersion = ModelVersion


  test("export string PropertyDefinition") {
    val stringPropertyDef = PropertyDefinition(
      name = "testStringProperty1",
      version = 2,
      description = Some("test desc"),
      propertyType = StringPropertyType(suggestedValue = "sort of default"),
      putIntoInfoFile = true,
      essentiality = Mandatory(false)
    )

    val expectedPropertyDef =
      s"""{"metadata":{"exportVersion":$modelVersion},"item":{
         |"name":"testStringProperty1",
         |"description":"test desc",
         |"propertyType":{"_t":"StringPropertyType","suggestedValue":"sort of default"},
         |"putIntoInfoFile":true,
         |"essentiality":{"_t":"Mandatory","allowRun":false}
         |}}""".stripMargin.replaceAll("[\\r\\n]", "")

    assert(stringPropertyDef.exportItem() == expectedPropertyDef)
  }

  test("export enum PropertyDefinition") {
    val enumPropertyDef = PropertyDefinition(
      name = "testEnumProperty1",
      version = 3,
      description = None,
      propertyType = EnumPropertyType(Set("optionA", "optionB", "optionC"), suggestedValue = "optionB")
    )

    val expectedPropertyDef =
      s"""{"metadata":{"exportVersion":$modelVersion},"item":{
         |"name":"testEnumProperty1",
         |"propertyType":{"_t":"EnumPropertyType","allowedValues":["optionA","optionB","optionC"],"suggestedValue":"optionB"},
         |"putIntoInfoFile":false,
         |"essentiality":{"_t":"Optional"}
         |}}""".stripMargin.replaceAll("[\\r\\n]", "")

    assert(enumPropertyDef.exportItem() == expectedPropertyDef)
  }

  test("Suggested value conformity should be checked") {
    val errorMessage = intercept[PropertyTypeValidationException] {
      PropertyDefinition(
        name = "testEnumProperty1",
        version = 3,
        description = None,
        propertyType = EnumPropertyType(Set("optionA", "optionB", "optionC"), suggestedValue = "invalidOption")
      )
    }.getMessage

    assert(errorMessage ==
      "The suggested value invalidOption cannot be used: Value 'invalidOption' is not one of the allowed values (optionA, optionB, optionC).")
  }

}
