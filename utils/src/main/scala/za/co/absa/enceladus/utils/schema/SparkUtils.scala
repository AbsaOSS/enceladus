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

package za.co.absa.enceladus.utils.schema

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Column, DataFrame}


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
    * @param col     An expression for the column to add
    * @return true if the column is the only column in a struct
    */
  def withColumnIfDoesNotExist(df: DataFrame, colName: String, col: Column): DataFrame = {
    if (df.schema.exists(field => field.name.equalsIgnoreCase(colName))) {
      log.warn(s"Column '$colName' already exists. The content of the column won't be overwritten.")
      df
    } else {
      df.withColumn(colName, col)
    }
  }
}
