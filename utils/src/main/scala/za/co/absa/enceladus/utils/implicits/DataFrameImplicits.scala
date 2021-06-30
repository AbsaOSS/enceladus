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

package za.co.absa.enceladus.utils.implicits

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.enceladus.utils.schema.{SchemaUtils, SparkUtils}

object DataFrameImplicits {
  implicit class DataFrameEnhancements(val df: DataFrame) {

    private def gatherData(showFnc: () => Unit): String = {
      val outCapture = new ByteArrayOutputStream
      Console.withOut(outCapture) {
        showFnc()
      }
      val dfData = new String(outCapture.toByteArray).replace("\r\n", "\n")
      dfData
    }

    def dataAsString(): String = {
      val showFnc: () => Unit = df.show
      gatherData(showFnc)
    }

    def dataAsString(truncate: Boolean): String = {
      val showFnc:  () => Unit = ()=>{df.show(truncate)}
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int, vertical: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate, vertical)
      gatherData(showFnc)
    }

    /**
      * Adds a column to a dataframe if it does not exist
      *
      * @param colName A column to add if it does not exist already
      * @param col     An expression for the column to add
      * @return a new dataframe with the new column
      */
    def withColumnIfDoesNotExist(colName: String, col: Column): DataFrame = {
      SparkUtils.withColumnIfDoesNotExist(df, colName, col)
    }

    /**
     * Set nullable property of column.
     *
     * @param columnName is the column name to change
     * @param nullable   boolean flag to set the nullability of the column `columnName` to
     */
    def setNullableStateOfColumn(columnName: String, nullable: Boolean): DataFrame = {
      // Courtesy of https://stackoverflow.com/a/33195510

      // modify [[StructField] with name `columnName`
      val newSchema = StructType(df.schema.map {
        case StructField(c, t, _, m) if c.equals(columnName) => StructField(c, t, nullable = nullable, m)
        case y: StructField => y
      })
      // apply new schema
      df.sqlContext.createDataFrame(df.rdd, newSchema)
    }

  }

}
