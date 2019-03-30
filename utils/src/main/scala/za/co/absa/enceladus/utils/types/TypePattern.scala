/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.utils.types

import org.apache.spark.sql.types.{DataType, StructField, DateType, TimestampType}
import za.co.absa.enceladus.utils.time.DateTimePattern

import scala.util.Try

/**
  * Class to carry enhanced information about formatting patterns in conversion from/to string
  * @param inputPattern  actual pattern to format the type conversion; if none global default pattern for the type is used
  * @param forType  type the format is intended for
  */
class TypePattern(inputPattern: Option[String], forType: Option[DataType] = None) {

  val pattern: String = inputPattern.getOrElse(Defaults.getGlobalFormat(forType.get))

  def isDefault: Boolean = inputPattern.isEmpty

  def getOrElse(default: String): String = inputPattern.getOrElse(default)
}

object TypePattern {
  implicit def patternToString(pattern: TypePattern): String = pattern.pattern

  private def getMetadata(structField: StructField, key: String): Option[String] = {
    Try(structField.metadata.getString(key)).toOption
  }

  def apply(structField: StructField ):TypePattern = {
    val dataType = structField.dataType
    val patternString: Option[String] = getMetadata(structField, "pattern")
    dataType match {
      case _: DateType | _: TimestampType =>
        val timeZone: Option[String] = getMetadata(structField,"timezone")
        new DateTimePattern(patternString, Some(dataType), timeZone)
      case _ => new TypePattern(patternString, Some(dataType))
    }
  }

  def apply(pattern: String, forType: Option[DataType] = None): TypePattern = {
    forType match {
      case Some(_: DateType) | Some(_: TimestampType) =>
        new DateTimePattern(Some(pattern), forType)
      case _ => new TypePattern(Some(pattern), forType)
    }
  }
}
