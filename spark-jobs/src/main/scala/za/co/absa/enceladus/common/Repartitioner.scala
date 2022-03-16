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

package za.co.absa.enceladus.common

import org.apache.spark.sql.DataFrame
import za.co.absa.enceladus.common.config.CommonConfConstants
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.spark.partition.sizing.DataFramePartitioner.DataFrameFunctions
import za.co.absa.spark.partition.sizing.sizer._
import za.co.absa.spark.partition.sizing.types.DataTypeSizes
import za.co.absa.spark.partition.sizing.types.DataTypeSizes.DefaultDataTypeSizes

class Repartitioner(configReader: ConfigReader) {

  val minPartition: Option[Long] = configReader.getLongOption(CommonConfConstants.minPartitionSizeKey)
  val maxPartition: Option[Long] = configReader.getLongOption(CommonConfConstants.maxPartitionSizeKey)

  implicit val dataTypeSizes: DataTypeSizes = DefaultDataTypeSizes

  def repartition(df: DataFrame): DataFrame = {
    val maybeString = configReader.getStringOption(CommonConfConstants.partitionStrategy)
    maybeString match {
      case Some("plan") => df.repartitionByPlanSize(minPartition, maxPartition)
      case Some("recordCount") => {
        val maybeInt = configReader.getLongOption(CommonConfConstants.maxRecordsPerPartitionKey)
        maybeInt match {
          case None => df
          case Some(x) => df.repartitionByRecordCount(x)
        }
      }
      case Some("schema") => {
        val sizer = new FromSchemaSizer()
        df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
      }
      case Some("dataframe") => {
        val sizer = new FromDataframeSizer()
        df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
      }
      case Some("sample") => {

        val maybeInt = configReader.getIntOption(CommonConfConstants.partitionSampleSizeKey)
        maybeInt match {
          case None => df
          case Some(x) => {
            val sizer = new FromDataframeSampleSizer(x)
            df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
          }
        }
      }
      case Some("schemaSummaries") => {
        val sizer = new FromSchemaWithSummariesSizer()
        df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
      }
      case _ => df
    }
  }

}
