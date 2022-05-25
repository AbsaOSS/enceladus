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
import org.slf4j.Logger
import za.co.absa.enceladus.common.config.CommonConfConstants
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.spark.partition.sizing.DataFramePartitioner.DataFrameFunctions
import za.co.absa.spark.partition.sizing.sizer._
import za.co.absa.spark.partition.sizing.types.DataTypeSizes
import za.co.absa.spark.partition.sizing.types.DataTypeSizes.DefaultDataTypeSizes

class Repartitioner(configReader: ConfigReader, log: Logger) {

  val minPartition: Option[Long] = configReader.getLongOption(CommonConfConstants.minPartitionSizeKey)
  val maxPartition: Option[Long] = configReader.getLongOption(CommonConfConstants.maxPartitionSizeKey)

  implicit val dataTypeSizes: DataTypeSizes = DefaultDataTypeSizes

  def repartition(df: DataFrame): DataFrame = {
    val partitionStrategy = configReader.getStringOption(CommonConfConstants.partitionStrategy)
    if (minPartition.isEmpty && maxPartition.isEmpty) {
      log.warn(s"No partitioning applied doe to missing: ${CommonConfConstants.minPartitionSizeKey}, " +
        s"${CommonConfConstants.minPartitionSizeKey} keys")
    }
    partitionStrategy match {
      case Some("plan") => df.repartitionByPlanSize(minPartition, maxPartition)
      case Some("dataframe") => repartitionByDf(df)
      case Some("sample") => repartitionBySample(df)
      case _ => df
    }
  }

  private def repartitionBySample(df: DataFrame): DataFrame = {
    val maybeInt = configReader.getIntOption(CommonConfConstants.partitionSampleSizeKey)
    maybeInt match {
      case None => {
        log.warn(s"No repartitioning applied due to missing ${CommonConfConstants.partitionSampleSizeKey} key")
        df
      }
      case Some(x) => {
        val sizer = new FromDataframeSampleSizer(x)
        df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
      }
    }
  }

  private def repartitionByDf(df: DataFrame): DataFrame = {
    val sizer = new FromDataframeSizer()
    df.repartitionByDesiredSize(sizer)(minPartition, maxPartition)
  }

}
