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

package za.co.absa.enceladus.utils.schema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.udf.ConformanceUDFLibrary
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.hats.transformations.NestedArrayTransformations


/**
  * General Spark utils
  */
object SparkUtils {
  private final val DefaultColumnNameOfCorruptRecord = "_corrupt_record"

  final val ColumnNameOfCorruptRecordConf = "spark.sql.columnNameOfCorruptRecord"

  /**
    * Ensures that the 'spark.sql.columnNameOfCorruptRecord' Spark setting is set to unique field name not present in the
    * provided schema
    * @param spark  the spark session to set the
    * @param schema the schema to check uniqueness against
    * @return the field name set
    */
  def setUniqueColumnNameOfCorruptRecord(spark: SparkSession, schema: StructType): String = {
    val result = if (schema.fieldExists(DefaultColumnNameOfCorruptRecord)) {
     schema.getClosestUniqueName(DefaultColumnNameOfCorruptRecord)
    } else {
      DefaultColumnNameOfCorruptRecord
    }
    spark.conf.set(ColumnNameOfCorruptRecordConf, result)
    result
  }

  /**
    * Overwrites a column with a value provided by an expression.
    * If the value in the column does not match the one provided by the expression, an error will be
    * added to the error column.
    *
    * @param df      A dataframe
    * @param colName A column to be overwritten
    * @param colExpr     An expression for the value to write
    * @return a new dataframe with the value of the column being overwritten
    */
  private def overwriteWithErrorColumn(df: DataFrame, colName: String, colExpr: Column): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    implicit val udfLib: ConformanceUDFLibrary = new ConformanceUDFLibrary


    val tmpColumn = df.schema.getClosestUniqueName("tmpColumn")
    val tmpErrColumn = df.schema.getClosestUniqueName("tmpErrColumn")
    val litErrUdfCall = call_udf("confLitErr", lit(colName), col(tmpColumn))

    // Rename the original column to a temporary name. We need it for comparison.
    val dfWithColRenamed = df.withColumnRenamed(colName, tmpColumn)

    // Add new column with the intended value
    val dfWithIntendedColumn = dfWithColRenamed.withColumn(colName, colExpr)

    // Add a temporary error column containing errors if the original value does not match the intended one
    val dfWithErrorColumn = dfWithIntendedColumn
      .withColumn(tmpErrColumn, array(when(col(tmpColumn) =!= colExpr, litErrUdfCall).otherwise(null))) // scalastyle:ignore null

    // Gather all errors in errCol
    val dfWithAggregatedErrColumn = NestedArrayTransformations
      .gatherErrors(dfWithErrorColumn, tmpErrColumn, ErrorMessage.errorColumnName)

    // Drop the temporary column
    dfWithAggregatedErrColumn.drop(tmpColumn)
  }

  implicit class DataFrameWithEnhancements(val df: DataFrame) {
    def withColumnOverwriteIfExists(colName: String, colExpr: Column): DataFrame = {
      val overwrite: (DataFrame, String) => DataFrame = overwriteWithErrorColumn(_, _, colExpr)
      df.withColumnIfDoesNotExist(overwrite)(colName, colExpr)
    }
  }

}
