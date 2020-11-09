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
    def isValueConforming(value: String): Boolean

    def typeSpecificSettings: Map[String, Any]
  }

  case class StringPropertyType() extends PropertyType {
    override def isValueConforming(value: String): Boolean = true

    override def typeSpecificSettings: Map[String, Any] = Map.empty
  }

  case class StringEnumPropertyType(allowedValues: Set[String]) extends PropertyType {

    override def isValueConforming(value: String): Boolean = allowedValues.contains(value)

    override def typeSpecificSettings: Map[String, Set[String]] = Map("items" -> allowedValues)
  }

}
