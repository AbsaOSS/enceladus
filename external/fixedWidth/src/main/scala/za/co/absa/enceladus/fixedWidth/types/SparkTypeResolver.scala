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


package za.co.absa.enceladus.fixedWidth.types

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object SparkTypeResolver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private[fixedWidth] def toSparkType(field: StructField, value: String): Any = {
    try {
      field.dataType match {
        case _: IntegerType => value.toInt
        case _: FloatType => value.toFloat
        case _: ShortType => value.toShort
        case _: DoubleType => value.toDouble
        case _: LongType => value.toLong
        case _: StringType => value
        case _: DateType | TimestampType => toSparkTimestamp(field, value)
        case _: BooleanType => value.toBoolean
        case _: DecimalType => new java.math.BigDecimal(value)
        case _ => throw new IllegalStateException(s"${field.dataType.typeName} is not a supported type in this version of fixed-width library")
      }
    }
    catch {
      case NonFatal(e) =>
        logger.error(s"Error during spark type casting for: ${field.dataType} $e")
        null
    }
  }

  private def getGlobalFormat(dt: DataType): String =
    dt match {
      case DateType      => "yyyy-MM-dd"
      case TimestampType => "yyyy-MM-dd HH:mm:ss"
      case _             => throw new IllegalStateException(s"No default format defined for data type ${dt.typeName}")
    }

  private def castToDateTimeType(field: StructField, dateString: String, pattern: String): Any = {

    val dateFormat = new SimpleDateFormat(pattern)

    try {
      val date = dateFormat.parse(dateString)
      field.dataType match {
        case _: TimestampType => new Timestamp(date.getTime)
        case _: DataType => new Date(date.getTime)
      }
    }
    catch {
      case NonFatal(e) => throw new IllegalStateException(s"Unable to parse the date string $dateString using pattern $pattern")
    }
  }

  private def toSparkTimestamp(field: StructField, dateVal: String): Any = {
    if (field.metadata contains "pattern") {
      castToDateTimeType(field, dateVal,field.metadata.getString("pattern"))
    }
    else {
      castToDateTimeType(field, dateVal,getGlobalFormat(field.dataType))
    }
  }
}
