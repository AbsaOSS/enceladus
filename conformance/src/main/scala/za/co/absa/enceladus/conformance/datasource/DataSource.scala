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

package za.co.absa.enceladus.conformance.datasource

import org.slf4j.LoggerFactory
import scala.collection.mutable.HashMap
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.text.MessageFormat
import org.apache.hadoop.fs.Path

/**
 * Utility object to provide access to data in HDFS (including partitioning and caching)
 */
object DataSource {
  private val log = LoggerFactory.getLogger("enceladus.conformance.DataSource")
  private val dfs = HashMap[String, Dataset[Row]]()

  /**
   * Get loaded dataframe or load data given the report date and partitioning pattern
   *
   * @param path The base path in HDFS of the data
   * @param reportYear String representing the year in `yyyy` format
   * @param reportMonth String representing the month in `MM` format
   * @param reportDay String representing the day in `dd` format
   * @param partitioningPattern Pattern representing the date partitioning where {0} stands for year, {1} for month, {2} for day
   * @return Dataframe with the required data (cached if requested more than once)
   */
  def getData(path: String, reportYear: String, reportMonth: String, reportDay: String, partitioningPattern: String)(implicit spark: SparkSession): Dataset[Row] = {
    if (dfs.contains(path)) {
      dfs(path).cache
    } else {
      val subPath = MessageFormat.format(partitioningPattern, reportYear, reportMonth, reportDay)
      val fillPath = if (subPath.isEmpty) new Path(path).toUri.toString else new Path(path, subPath).toUri.toString
      log.info(s"Mapping table used: $fillPath")
      val df = spark.read.parquet(fillPath)
      dfs += (path -> df)
      df
    }
  }

  def getData(path: String, reportDate: String, partitioningPattern: String)(implicit spark: SparkSession): Dataset[Row] = {
    val dateTokens = reportDate.split("-")
    getData(path, dateTokens(0), dateTokens(1), dateTokens(2), partitioningPattern)
  }

  private[conformance] def setData(path: String, data: Dataset[Row]) {
    dfs += (path -> data)
  }
}
