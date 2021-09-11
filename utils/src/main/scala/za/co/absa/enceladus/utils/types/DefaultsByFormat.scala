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

package za.co.absa.enceladus.utils.types

import org.apache.spark.sql.types.DataType
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.enceladus.utils.numeric.DecimalSymbols

import java.util.TimeZone
import scala.util.Try

class DefaultsByFormat(formatName: String,
                       globalDefaults: Defaults = GlobalDefaults,
                       private val config: ConfigReader = new ConfigReader()) extends  Defaults {

  /** A function which defines default values for primitive types */
  override def getDataTypeDefaultValue(dt: DataType): Any = globalDefaults.getDataTypeDefaultValue(dt)

  /** A function which defines default values for primitive types, allowing possible Null */
  override def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]] = {
    globalDefaults.getDataTypeDefaultValueWithNull(dt, nullable)
  }

  /** A function which defines default formats for primitive types */
  override def getStringPattern(dt: DataType): String = {
    globalDefaults.getStringPattern(dt)
  }

  override def getDefaultTimestampTimeZone: Option[String] = {
    defaultTimestampTimeZone.orElse(globalDefaults.getDefaultTimestampTimeZone)
  }

  override def getDefaultDateTimeZone: Option[String] = {
    defaultDateTimeZone.orElse(globalDefaults.getDefaultDateTimeZone)
  }

  override def getDecimalSymbols: DecimalSymbols = globalDefaults.getDecimalSymbols

  private def readTimezone(path: String): Option[String] = {
    val result = config.readStringConfigIfExist(path)
    result.foreach(tz =>
      if (!TimeZone.getAvailableIDs().contains(tz )) {
        throw new IllegalStateException(s"The setting '$tz' of '$path' is not recognized as known time zone")
      }
    )
    result
  }

  private def formatSpecificConfigurationName(configurationName: String): String = {
    s"$configurationName-$formatName"
  }

  private val defaultTimestampTimeZone: Option[String] =
    readTimezone(formatSpecificConfigurationName(DefaultsByFormat.TimestampTimeZoneName))
      .orElse(readTimezone(DefaultsByFormat.TimestampTimeZoneName))

  private val defaultDateTimeZone: Option[String] =
    readTimezone(formatSpecificConfigurationName(DefaultsByFormat.DateTimeZoneName))
      .orElse(readTimezone(DefaultsByFormat.DateTimeZoneName))
}

object DefaultsByFormat {
  final val TimestampTimeZoneName = "defaultTimestampTimeZone"
  final val DateTimeZoneName = "defaultDateTimeZone"
}
