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

package za.co.absa.enceladus.model.properties

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

package object propertyType {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
  @JsonSubTypes(Array(
    new Type(value = classOf[StringPropertyType], name = "StringPropertyType"),
    new Type(value = classOf[StringEnumPropertyType], name = "StringEnumPropertyType")
  ))
  sealed trait PropertyType {
    def suggestedValue: String

    def isValueConforming(value: String): Boolean
  }

  case class StringPropertyType(suggestedValue: String = "") extends PropertyType {
    override def isValueConforming(value: String): Boolean = true
  }

  case class StringEnumPropertyType(allowedValues: Set[String], suggestedValue: String) extends PropertyType {
    override def isValueConforming(value: String): Boolean = allowedValues.contains(value)

    require(isValueConforming(suggestedValue), s"The suggested value '$suggestedValue' does not conform to the propertyType $this!")
  }

  object StringEnumPropertyType {
    /**
     * Shorthand for creating [[StringEnumPropertyType]].
     * @param allowedValues The first value will be assinged to the [[PropertyType#suggestedValue]] field.
     * @return
     */
    def apply(allowedValues: String*): StringEnumPropertyType = StringEnumPropertyType(allowedValues.toSet, allowedValues(0))
  }

}
