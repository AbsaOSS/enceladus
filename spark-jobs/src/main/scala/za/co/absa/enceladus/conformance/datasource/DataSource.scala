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

package za.co.absa.enceladus.conformance.datasource

import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Utility object to provide access to data in HDFS (including partitioning and caching)
  */
object DataSource {
  private val log = LoggerFactory.getLogger("enceladus.conformance.DataSource")
  private val dfs = mutable.HashMap[String, Dataset[Row]]()

  /**
    * Get loaded dataframe or load data given the report date.
    * The partitioning pattern is determined by the current configuration.
    *
    * @param path       The base path in HDFS of the data
    * @param reportDate A string representing a report date in `yyyy-mm-dd` format
    * @return Dataframe with the required data (cached if requested more than once)
    */
  def getDataFrame(path: String,
                   reportDate: String)
                  (implicit spark: SparkSession): Dataset[Row] = {
    getDataFrame(path, reportDate, PartitioningUtils.mappingTablePattern)
  }


  /**
    * Get loaded dataframe or load data given the report date and partitioning pattern.
    *
    * @param path                The base path in HDFS of the data
    * @param reportDate          A string representing a report date in `yyyy-mm-dd` format
    * @param partitioningPattern Pattern representing the date partitioning where {0} stands for year, {1} for month, {2} for day
    * @return Dataframe with the required data (cached if requested more than once)
    */
  def getDataFrame(path: String,
                   reportDate: String,
                   partitioningPattern: String)
                  (implicit spark: SparkSession): Dataset[Row] = {
    if (dfs.contains(path)) {
      dfs(path)
    } else {
      val fullPath = PartitioningUtils.getPartitionedPathName(path, reportDate, partitioningPattern)
      log.info(s"Use partitioned path for: '$path' -> '$fullPath'")
      val df = loadDataFrame(fullPath)
      dfs += (path -> df)
      df
    }
  }

  private[conformance] def setData(path: String, data: Dataset[Row]) {
    dfs += (path -> data)
  }

  private def loadDataFrame(path: String)
                           (implicit spark: SparkSession): DataFrame = {
    try {
      spark.read.parquet(path)
    } catch {
      case ex: AnalysisException if ex.getMessage.contains("Unable to infer schema for Parquet") =>
        throw new RuntimeException(s"Unable to read the mapping table from '$path'. " +
          "Possibly, the directory does not have valid Parquet files.", ex)
      case NonFatal(e) =>
        throw e
    }
  }
}
