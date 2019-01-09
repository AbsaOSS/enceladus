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

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.annotation.switch

class DataframeReader(datasetPath: String, inputSchema: Option[StructType] )(implicit cmd: CmdConfig, sparkSession: SparkSession) {
  lazy val dataFrame: DataFrame = getDataFrameReader.load(datasetPath)
  lazy val dataFrameSchema: StructType = dataFrame.schema

  def getSchemaWithoutMetadata: StructType = {
    StructType(dataFrameSchema.map{ f => StructField(f.name, f.dataType, f.nullable) })
  }

  private def getDataFrameReader(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    (cmd.rawFormat: @switch) match {
      case "csv" => getCsvReader
      case "xml" => getXmlReader
      case "fixed-width" => getFixedWidthReader
      case "parquet" => getParquetReader
      case _ => getStandardReader
    }
  }

  private def getStandardReader()(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    sparkSession.read.format(cmd.rawFormat)
  }

  private def getParquetReader()(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader

    if (inputSchema.isDefined) dfReader.schema(inputSchema.get)
    else dfReader
  }

  private def getFixedWidthReader()(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader

    if (cmd.fixedWidthTrimValues.get) dfReader.option("trimValues", "true")
    else dfReader
  }

  private def getXmlReader()(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    getStandardReader.option("rowTag", cmd.rowTag.get)
  }

  private def getCsvReader()(implicit cmd: CmdConfig, sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader.option("delimiter", cmd.csvDelimiter.get)

    if (cmd.csvHeader.isDefined) dfReader.option("header", cmd.csvHeader.get)
    else dfReader
  }
}
