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

package za.co.absa.enceladus.utils.library

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.spark_partition_id

package object sizer {
  type ByteSize = Long

  case class PartitionRecordCount(partitionId: Int, recordCount: Long)

  def dataFrameParitionRecordCount(df: DataFrame): Array[PartitionRecordCount] = {
    // this function was moved out from `DataFrameFunctions` because `PartitionRecordCount` cannot be used withing an
    // implicit class extending `AnyVal`
    import df.sqlContext.implicits._
    df.groupBy(spark_partition_id)
      .count()
      .as[PartitionRecordCount]
      .collect()
  }
}
