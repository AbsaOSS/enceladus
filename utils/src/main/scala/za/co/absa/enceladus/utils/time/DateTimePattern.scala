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

package za.co.absa.enceladus.utils.time

import java.security.InvalidParameterException

import org.apache.spark.sql.types.{DataType, StructField}
import za.co.absa.enceladus.utils.types.Defaults

import scala.util.Try

/**
  * Class to carry enhanced information about formatting patterns in conversion from/to string
  * @param pattern  the actual pattern to format the type conversion; if none global default pattern for the type is used
  * @param forType  the type the format is intended for
  */
class DateTimePattern(val pattern: Option[String], val forType: Option[DataType] = None){
  private val actualFormat: String = pattern.getOrElse(Defaults.getGlobalFormat(forType.get))

  def isDefault: Boolean = pattern.isEmpty
  def get: String = actualFormat
  def getOrElse(default: String): String =  pattern.getOrElse(default)

  def isEpoch: Boolean = {
    DateTimePattern.isEpoch(get)
  }

  def epochFactor: Long = {
    DateTimePattern.epochFactor(get)
  }

  def epochMilliFactor: Long = {
    DateTimePattern.epochMilliFactor(get)
  }
}

object DateTimePattern {
  private val thousand = 1000

  def apply(structField: StructField ): DateTimePattern = {
    val formatString: Option[String] = Try(structField.metadata.getString("pattern")).toOption
    val dataType: DataType = structField.dataType
    new DateTimePattern(formatString, Some(dataType))
  }

  def apply(pattern: String, forType: Option[DataType] = None): DateTimePattern = {
    new DateTimePattern(Some(pattern), forType)
  }

  def isEpoch(format: String): Boolean = {
    format.toLowerCase match {
      case "epoch" | "epochmilli" => true
      case _ => false
    }
  }

  def epochFactor(format: String): Long = {
    format.toLowerCase match {
      case "epoch"      => 1
      case "epochmilli" => thousand
      case _            => throw new InvalidParameterException(s"'$format' is not an epoch format")
    }
  }

  def epochMilliFactor(format: String): Long = {
    format.toLowerCase match {
      case "epoch"      => thousand
      case "epochmilli" => 1
      case _            => throw new InvalidParameterException(s"'$format' is not an epoch format")
    }
  }

  implicit def format2String(format: DateTimePattern): String = format.get

}
