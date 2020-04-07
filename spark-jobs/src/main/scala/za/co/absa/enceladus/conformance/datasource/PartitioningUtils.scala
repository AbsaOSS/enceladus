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

import java.text.MessageFormat

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path

object PartitioningUtils {

  private val conf = ConfigFactory.load()

  // Pattern representing the date partitioning where {0} stands for year, {1} for month, {2} for day
  lazy val mappingTablePattern: String = conf.getString("conformance.mappingtable.pattern")

  /**
    * Returns a partitioned path given a path and a report date.
    * The partitioning pattern is determined by the configuration parameter.
    *
    * @param reportDate A report date in 'yyyy-MM-dd' format
    * @return A full path to the partitioned data.
    */
  def getPartitionedPathName(path: String,
                             reportDate: String): String = {
    getPartitionedPathName(path, reportDate, mappingTablePattern)
  }

  /**
    * Returns a partitioned path given a report date and partitioning pattern.
    *
    * @param reportDate          A report date in 'yyyy-MM-dd' format
    * @param partitioningPattern A pattern representing the date partitioning where {0} stands for year, {1} for month, {2} for day
    * @return A full path to the partitioned data.
    */
  def getPartitionedPathName(path: String,
                             reportDate: String,
                             partitioningPattern: String): String = {
    validateReportDate(reportDate)
    val dateTokens = reportDate.split("-")

    val reportYear = dateTokens(0)
    val reportMonth = dateTokens(1)
    val reportDay = dateTokens(2)

    val subPath = getPartitionSubPath(reportYear, reportMonth, reportDay, partitioningPattern)
    val fullPath = if (subPath.isEmpty) new Path(path).toUri.toString else new Path(path, subPath).toUri.toString

    fullPath
  }

  /**
    * Returns a partition subpath given a report date and partitioning pattern.
    *
    * @param reportYear          A string representing the year in `yyyy` format
    * @param reportMonth         A string representing the month in `MM` format
    * @param reportDay           A string representing the day in `dd` format
    * @param partitioningPattern A pattern representing the date partitioning where {0} stands for year, {1} for month, {2} for day
    * @return A subpath as a string
    */
  def getPartitionSubPath(reportYear: String,
                          reportMonth: String,
                          reportDay: String,
                          partitioningPattern: String): String = {
    MessageFormat.format(partitioningPattern, reportYear, reportMonth, reportDay)
  }

  private def validateReportDate(reportDate: String): Unit = {
    val reportDateRegEx = """\d\d\d\d-\d\d-\d\d""".r

    if (!reportDateRegEx.pattern.matcher(reportDate).matches()) {
      throw new IllegalArgumentException(s"A report date '$reportDate' does not match expected pattern 'yyyy-MM-dd'")
    }
  }

}
