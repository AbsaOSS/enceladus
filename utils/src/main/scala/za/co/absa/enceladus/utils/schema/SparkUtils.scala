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

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations


/**
  * General Spark utils
  */
object SparkUtils {
  private val log: Logger = LogManager.getLogger(this.getClass)

  /**
    * Adds a column to a dataframe if it does not exist
    *
    * @param df      A dataframe
    * @param colName A column to add if it does not exist already
    * @param colExpr An expression for the column to add
    * @return a new dataframe with the new column
    */
  def withColumnIfDoesNotExist(df: DataFrame, colName: String, colExpr: Column): DataFrame = {
    if (df.schema.exists(field => field.name.equalsIgnoreCase(colName))) {
      log.warn(s"Column '$colName' already exists. The content of the column will be overwritten.")
      overwriteWithErrorColumn(df, colName, colExpr)
    } else {
      df.withColumn(colName, colExpr)
    }
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
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary


    val tmpColumn = SchemaUtils.getUniqueName("tmpColumn", Some(df.schema))
    val tmpErrColumn = SchemaUtils.getUniqueName("tmpErrColumn", Some(df.schema))
    val litErrUdfCall = callUDF("confLitErr", lit(colName), col(tmpColumn))

    // Rename the original column to a temporary name. We need it for comparison.
    val dfWithColRenamed = df.withColumnRenamed(colName, tmpColumn)

    // Add new column with the intended value
    val dfWithIntendedColumn = dfWithColRenamed.withColumn(colName, colExpr)

    // Add a temporary error column containing errors if the original value does not match the intended one
    val dfWithErrorColumn = dfWithIntendedColumn
      .withColumn(tmpErrColumn, array(when(col(tmpColumn) =!= colExpr, litErrUdfCall).otherwise(null))) // scalastyle:ignore null

    // Gather all errors in errCol
    val dfWithAggregatedErrColumn = DeepArrayTransformations
      .gatherErrors(dfWithErrorColumn, tmpErrColumn, ErrorMessage.errorColumnName)

    // Drop the temporary column
    dfWithAggregatedErrColumn.drop(tmpColumn)
  }

}
