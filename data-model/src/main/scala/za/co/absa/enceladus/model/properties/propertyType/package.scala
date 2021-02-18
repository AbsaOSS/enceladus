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

import scala.util.{Failure, Success, Try}

package object propertyType {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
  @JsonSubTypes(Array(
    new Type(value = classOf[StringPropertyType], name = "StringPropertyType"),
    new Type(value = classOf[EnumPropertyType], name = "EnumPropertyType")
  ))
  sealed trait PropertyType {
    def suggestedValue: String

    def isValueConforming(value: String): Try[Unit]
  }

  case class StringPropertyType(suggestedValue: String = "") extends PropertyType {
    override def isValueConforming(value: String): Try[Unit] = Success(Unit)
  }

  case class EnumPropertyType(allowedValues: Set[String], suggestedValue: String) extends PropertyType {
    override def isValueConforming(value: String): Try[Unit] = {
      if (allowedValues.contains(value)) {
        Success(Unit)
      } else {
        Failure(new IllegalArgumentException(s"Value '$value' is not one of the allowed values (${allowedValues.mkString(", ")})."))
      }
    }

    isValueConforming(suggestedValue) match {
      case Success(_) =>
      case Failure(e) => throw PropertyTypeValidationException(s"The suggested value $suggestedValue cannot be used: ${e.getMessage}", e)
    }
  }

  object EnumPropertyType {
    /**
     * Shorthand for creating [[EnumPropertyType]].
     * @param allowedValues The first value will be assinged to the [[PropertyType#suggestedValue]] field.
     * @return
     */
    def apply(allowedValues: String*): EnumPropertyType = EnumPropertyType(allowedValues.toSet, allowedValues(0))
  }

  case class PropertyTypeValidationException(msg: String, cause: Throwable) extends Exception(msg, cause)

}
