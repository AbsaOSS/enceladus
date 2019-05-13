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

package za.co.absa.enceladus.testutils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{expr, max}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object HelperFunctions {
  /**
    * Taken from AbsaOSS/Cobrix project spark-cobol utils
    * @see https://github.com/AbsaOSS/cobrix
    *
    * Given an instance of [[DataFrame]] returns a list of columns for flattening.
    * For how to use look at [[flattenDataFrame()]]
    * All nested structures can be then flattened and arrays are projected as columns.
    *
    * Note. The method checks the maximum size for each array and that could perform slowly,
    * especially on a vary big dataframes.
    *
    * @param df A dataframe
    * @return A new dataframe with flat schema.
    */
  def flattenSchema(df: DataFrame): List[Column] = {
    val logger: Logger = LogManager.getLogger("enceladus.testutils.Flattening")
    val fields = new mutable.ListBuffer[Column]()
    val stringFields = new mutable.ListBuffer[String]()

    /**
      * Aggregating arrays of primitives by projecting it's columns
      *
      * @param path path to an StructArray
      * @param fieldNamePrefix Prefix for the field name
      * @param structField StructField
      * @param arrayType ArrayType
      */
    def flattenStructArray(path: String, fieldNamePrefix: String, structField: StructField, arrayType: ArrayType): Unit = {
      val maxInd = df.agg(max(expr(s"size($path${structField.name})"))).collect()(0)(0).toString.toInt
      var i = 0
      while (i < maxInd) {
        val newFieldNamePrefix = s"$fieldNamePrefix${i}_"
        arrayType.elementType match {
          case st: StructType =>
            flattenGroup(s"$path`${structField.name}`[$i].", newFieldNamePrefix, st)
          case ar: ArrayType =>
            flattenArray(s"$path`${structField.name}`[$i].", newFieldNamePrefix, structField, ar)
          case _ =>
            val newFieldName = s"$fieldNamePrefix$i"
            fields += expr(s"$path`${structField.name}`[$i]").as(newFieldName)
            stringFields += s"""expr("$path`${structField.name}`[$i] AS `$newFieldName`")"""
        }
        i += 1
      }
    }

    def flattenNestedArrays(path: String, fieldNamePrefix: String, arrayType: ArrayType): Unit = {
      val maxIndexes = df.agg(max(expr(s"size($path)"))).collect()(0)(0).toString.toInt
      var i = 0
      while (i < maxIndexes) {
        val newFieldNamePrefix = s"$fieldNamePrefix${i}_"
        arrayType.elementType match {
          case st: StructType =>
            flattenGroup(s"$path[$i]", newFieldNamePrefix, st)
          case ar: ArrayType =>
            flattenNestedArrays(s"$path[$i]", newFieldNamePrefix, ar)
          case _ =>
            val newFieldName = s"$fieldNamePrefix$i"
            fields += expr(s"$path[$i]").as(newFieldName)
            stringFields += s"""expr("$path`[$i] AS `$newFieldName`")"""
        }
        i += 1
      }
    }

    def flattenArray(path: String, fieldNamePrefix: String, structField: StructField, arrayType: ArrayType): Unit = {
      arrayType.elementType match {
        case _: StructType =>
          flattenStructArray(path, fieldNamePrefix, structField, arrayType)
        case _: ArrayType =>
          flattenNestedArrays(s"$path${structField.name}", fieldNamePrefix, arrayType)
        case _ =>
          flattenStructArray(path, fieldNamePrefix, structField, arrayType)
      }
    }

    def flattenGroup(path: String, fieldNamePrefix: String, structField: StructType): Unit = {
      structField.foreach(field => {
        val newFieldNamePrefix = s"$fieldNamePrefix${field.name}_"
        field.dataType match {
          case st: StructType =>
            flattenGroup(s"$path`${field.name}`.", newFieldNamePrefix, st)
          case arr: ArrayType =>
            flattenArray(path, newFieldNamePrefix, field, arr)
          case _ =>
            val newFieldName = s"$fieldNamePrefix${field.name}"
            fields += expr(s"$path`${field.name}`").as(newFieldName)
            if (path.contains('['))
              stringFields += s"""expr("$path`${field.name}` AS `$newFieldName`")"""
            else
              stringFields += s"""col("$path`${field.name}`").as("$newFieldName")"""
        }
      })
    }

    flattenGroup("", "", df.schema)
    logger.info(stringFields.mkString("Flattening code: \n.select(\n", ",\n", "\n)"))
    fields.toList
  }

  def flattenDataFrame(df: DataFrame): DataFrame = {
    val flatteningFormula: List[Column] = flattenSchema(df)
    df.select(flatteningFormula: _*)
  }

  /**
    * Calculates the time it took for passed block of code to execute and finish.
    * @param processToRun Block of code, to be executed
    * @tparam A A type parameter specifying output from processToRun
    * @return Returns a tuple of milliseconds difference between start and end time
    *         and return value
    */
  def calculateTime[A](processToRun: => A): (Long, A) = {
    val startTime = System.nanoTime()
    val returnValue = processToRun
    val endTime = System.nanoTime()
    val millisecondsPassed = (endTime - startTime) / 1000000
    (millisecondsPassed, returnValue)
  }

  /**
    * Pretty prints elapsed time. If given 91441000 will return "1 day, 1 hour, 24 minutes and 1.00 second"
    * @param elapsedTime Elapsed time in milliseconds you want to pretty print
    * @return Returns a string format with human readable time segments
    */
  def prettyPrintElapsedTime(elapsedTime: Long): String = {
    def stringify(count: Double, noun: String, formatter: String = "%.0f"): Option[String] = {
      if (count == 0) { None }
      else {
        val formatted = formatter.format(count)
        Some(s"$formatted $noun${if (count == 1) { "" } else { "s" }}")
      }
    }

    def extractTimeSegment(remaining: Long, multiplier: Long): (Long, Long) = {
      val count = remaining / multiplier
      val newRemaining = remaining - (count * multiplier)
      (count, newRemaining)
    }

    val minuteMultiplier: Int = 1000 * 60
    val hourMultiplier: Int = minuteMultiplier * 60
    val dayMultiplier: Int = hourMultiplier * 24

    val (numberOfDays, remainingAfterDay) = extractTimeSegment(elapsedTime, dayMultiplier)
    val (numberOfHours, remainingAfterHours) = extractTimeSegment(remainingAfterDay, hourMultiplier)
    val (numberOfMinutes, remainingAfterMinutes) = extractTimeSegment(remainingAfterHours, minuteMultiplier)
    val numberOfSeconds = remainingAfterMinutes / 1000.0

    val daysString = stringify(numberOfDays, "day")
    val hoursString = stringify(numberOfHours, "hour")
    val minutesString = stringify(numberOfMinutes, "minute")
    val secondsString = stringify(numberOfSeconds, "second", "%.2f")

    val returnVal = Array(daysString, hoursString, minutesString, secondsString).flatten.mkString(", ")
    val patchIndex = returnVal.lastIndexOf(',')
    if (patchIndex == -1) { returnVal }
    else { returnVal.patch(returnVal.lastIndexOf(','), " and", 1) }
  }
}
