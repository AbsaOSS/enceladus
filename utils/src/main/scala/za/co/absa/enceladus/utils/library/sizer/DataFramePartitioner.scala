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

package za.co.absa.enceladus.utils.library.sizer

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.spark_partition_id


// TODO add (optional) skew suppression

object DataFramePartitioner {
  implicit class DataFrameFunctions(val df: DataFrame) extends AnyVal {

    def cacheIfNot(): DataFrame = {
      val planToCache = df.queryExecution.analyzed
      if (df.sparkSession.sharedState.cacheManager.lookupCachedData(planToCache).isEmpty) {
        df.cache()
      } else {
        df
      }
    }

    def partitionsRecordCount: Map[Int, Long] = {
      dataFrameParitionRecordCount(df)
        .map(x => (x.partitionId, x.recordCount))
        .toMap
    }

    def recordCount: Long = {
      partitionsRecordCount.values.sum
    }

    def repartitionByRecordCount(maxRecordsPerPartition: Long): DataFrame = {
      val partitionCountLong = (recordCount / maxRecordsPerPartition) +
        (if (recordCount % maxRecordsPerPartition == 0) 0 else 1)
      val partitionCount: Int = partitionCountLong match {
        case x if x < 1 => 1
        case x if x > Int.MaxValue => Int.MaxValue
        case x => x.toInt
      }
      df.repartition(partitionCount)
    }

    def repartitionByPlanSize(minPartitionSize: Option[ByteSize], maxPartitionSize: Option[ByteSize]): DataFrame = {

      def computeBlockCount(totalByteSize: BigInt, desiredSize: Long, addRemainder: Boolean): Int = {
        val int = (totalByteSize / desiredSize).toInt
        val blockCount = int + (if (addRemainder && (totalByteSize % desiredSize != 0)) 1 else 0)
        blockCount max 1
      }

      df.cacheIfNot()
      val catalystPlan = df.queryExecution.logical
      val sizeInBytes = df.sparkSession.sessionState.executePlan(catalystPlan).optimizedPlan.stats.sizeInBytes

      val currentBlockSize = sizeInBytes / df.rdd.getNumPartitions

      (minPartitionSize, maxPartitionSize) match {
        case (Some(min), None) if currentBlockSize < min =>
          df.repartition(computeBlockCount(sizeInBytes, min, addRemainder = false))
        case (None, Some(max)) if currentBlockSize > max =>
          df.repartition(computeBlockCount(sizeInBytes, max, addRemainder = true))
        case (Some(min), Some(max)) if currentBlockSize < min || currentBlockSize > max =>
          df.repartition(computeBlockCount(sizeInBytes, max, addRemainder = true))
        case _ => df
      }
    }

    def repartitionToFileSize(minPartitionSize: Option[ByteSize], maxPartitionSize: Option[ByteSize]): DataFrame = {
      ??? // question if easily possible
    }
  }
}
