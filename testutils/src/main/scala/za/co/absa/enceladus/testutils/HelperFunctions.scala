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
import za.co.absa.enceladus.testutils.models.ProcessMeasurement

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
    * Calculates times that it took for passed block of code to execute and finish.
    * @param processToRun Block of code, to be executed
    * @tparam A A type parameter specifying callback return
    * @return Returns a millisecond difference between start and end time
    */
  def calculateTime[A](processToRun: => A): ProcessMeasurement[A] = {
    val startTime = System.nanoTime()
    val returnValue = processToRun
    val endTime = System.nanoTime()
    ProcessMeasurement(endTime - startTime, returnValue)

  }
}
