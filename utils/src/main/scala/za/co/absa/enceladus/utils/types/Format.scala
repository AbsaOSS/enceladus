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

import java.security.InvalidParameterException
import org.apache.spark.sql.types.{DataType, StructField}
import scala.util.Try

/**
  * Class to carry enhanced information about formatting patterns in conversion from/to string
  * @param pattern  the actual pattern to format the type conversion; if none global default pattern for the type is used
  * @param forType  the type the format is intended for
  */
class Format(val pattern: Option[String], val forType: Option[DataType] = None){
  private val actualFormat: String = pattern.getOrElse(Defaults.getGlobalFormat(forType.get))

  def isDefault: Boolean = pattern.isEmpty
  def get: String = actualFormat
  def getOrElse(default: String): String =  pattern.getOrElse(default)

  def isEpoch: Boolean = {
    Format.isEpoch(get)
  }

  def epochFactor: Long = {
    Format.epochFactor(get)
  }
}

object Format {
  def apply(structField: StructField ): Format = {
    val formatString: Option[String] = Try(structField.metadata.getString("pattern")).toOption
    val dataType: DataType = structField.dataType
    new Format(formatString, Some(dataType))
  }

  def apply(pattern: String, forType: Option[DataType] = None): Format = {
    new Format(Some(pattern), forType)
  }

  def isEpoch(format: String): Boolean = {
    format.toLowerCase match {
      case "epoch" | "milliepoch" => true
      case _ => false
    }
  }

  def epochFactor(format: String): Long = {
    format.toLowerCase match {
      case "epoch"      => 1
      case "milliepoch" => 1000
      case _            => throw new InvalidParameterException(s"'$format' is not an epoch format")
    }
  }

  implicit def format2String(format: Format): String = format.get

}
