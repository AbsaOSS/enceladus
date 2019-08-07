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

import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.types._
import scala.util.{Success, Try}

object Defaults {
  /** A function which defines default values for primitive types */
  def getGlobalDefault(dt: DataType): Any =
    dt match {
      case _: IntegerType   => 0
      case _: FloatType     => 0f
      case _: ByteType      => 0.toByte
      case _: ShortType     => 0.toShort
      case _: DoubleType    => 0.0d
      case _: LongType      => 0L
      case _: StringType    => ""
      case _: DateType      => new Date(0) //linux epoch
      case _: TimestampType => new Timestamp(0)
      case _: BooleanType   => false
      case t: DecimalType   =>
        val rest = t.precision - t.scale
        new java.math.BigDecimal(("0" * rest) + "." + ("0" * t.scale))
      case _                => throw new IllegalStateException(s"No default value defined for data type ${dt.typeName}")
    }

  /** A function which defines default values for primitive types, allowing possible Null*/
  def getGlobalDefaultWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]] = {
    val result = if (nullable) {
      Success(None)
    } else {
      Try{
        getGlobalDefault(dt)
      }.map(Some(_))
    }
    result
  }

  /** A function which defines default formats for primitive types */
  def getGlobalFormat(dt: DataType): String =
    dt match {
      case DateType      => "yyyy-MM-dd"
      case TimestampType => "yyyy-MM-dd HH:mm:ss"
      case _             => throw new IllegalStateException(s"No default format defined for data type ${dt.typeName}")
    }
}

