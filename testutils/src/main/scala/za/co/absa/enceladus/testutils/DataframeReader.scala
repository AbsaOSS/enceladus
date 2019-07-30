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

case class DataframeReaderOptions(rawFormat: String = "xml",
                                  rowTag: Option[String] = None,
                                  csvDelimiter: Option[String] = None,
                                  csvHeader: Option[Boolean] = Some(false),
                                  fixedWidthTrimValues: Option[Boolean] = Some(false))

class DataframeReader(datasetPath: String, inputSchema: Option[StructType] )
                     (implicit dfReaderOptions: DataframeReaderOptions, sparkSession: SparkSession) {
  lazy val dataFrame: DataFrame = getDataFrameReader.load(datasetPath)
  lazy val dataFrameSchema: StructType = dataFrame.schema

  def getSchemaWithoutMetadata: StructType = {
    StructType(dataFrameSchema.map{ f => StructField(f.name, f.dataType, f.nullable) })
  }

  private def getDataFrameReader(implicit dfReaderOptions: DataframeReaderOptions,
                                 sparkSession: SparkSession): DataFrameReader = {
    dfReaderOptions.rawFormat match {
      case "csv" => getCsvReader
      case "xml" => getXmlReader
      case "fixed-width" => getFixedWidthReader
      case "parquet" => getParquetReader
      case _ => getStandardReader
    }
  }

  private def getStandardReader()(implicit dfReaderOptions: DataframeReaderOptions,
                                  sparkSession: SparkSession): DataFrameReader = {
    sparkSession.read.format(dfReaderOptions.rawFormat)
  }

  private def getParquetReader()(implicit dfReaderOptions: DataframeReaderOptions,
                                 sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader

    inputSchema match {
      case Some(schema) => dfReader.schema(schema)
      case None         => dfReader
    }
  }

  private def getFixedWidthReader()(implicit dfReaderOptions: DataframeReaderOptions,
                                    sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader

    dfReaderOptions.fixedWidthTrimValues match {
      case Some(trimValues) => dfReader.option("trimValues", trimValues)
      case None             => dfReader
    }
  }

  private def getXmlReader()(implicit dfReaderOptions: DataframeReaderOptions,
                             sparkSession: SparkSession): DataFrameReader = {
    getStandardReader.option("rowTag", dfReaderOptions.rowTag.get)
  }

  private def getCsvReader()(implicit dfReaderOptions: DataframeReaderOptions,
                             sparkSession: SparkSession): DataFrameReader = {
    val dfReader = getStandardReader.option("delimiter", dfReaderOptions.csvDelimiter.get)

    dfReaderOptions.csvHeader match {
      case Some(hasHeader) => dfReader.option("header", hasHeader)
      case None            => dfReader
    }
  }
}
