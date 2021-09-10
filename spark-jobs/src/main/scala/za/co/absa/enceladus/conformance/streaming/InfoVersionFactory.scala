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

package za.co.absa.enceladus.conformance.streaming

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructField
import za.co.absa.enceladus.conformance.streaming.InfoDateFactory.log
import za.co.absa.enceladus.utils.schema.SchemaUtils

sealed trait InfoVersionFactory {
  def getInfoVersionColumn(df: DataFrame): Column
}

object InfoVersionFactory {

  private class InfoVersionLiteralFactory(reportVersion: Int) extends InfoVersionFactory {
    override def getInfoVersionColumn(df: DataFrame): Column = lit(reportVersion)
  }

  private class InfoVersionColumnFactory(columnName: String) extends InfoVersionFactory {
    override def getInfoVersionColumn(df: DataFrame): Column = {
      val dt: Option[StructField] = SchemaUtils.getField(columnName, df.schema)
      dt match {
        case Some(_) => col(columnName)
        case None => throw new IllegalArgumentException(s"The specified info column does not exist: $columnName")
      }
    }
  }

  import za.co.absa.enceladus.conformance.HyperConformanceAttributes._

  def getFactoryFromConfig(conf: Configuration): InfoVersionFactory = {
    if (conf.containsKey(reportVersionKey)) {
      if (conf.containsKey(reportVersionColumnKey)) {
        log.warn(s"Both $reportVersionKey and $reportVersionColumnKey specified, applying literal")
      }
      val reportVersion = conf.getInt(reportVersionKey)
      log.info(s"Information version: Explicit from the job configuration = $reportVersion")
      new InfoVersionLiteralFactory(reportVersion)
    } else if (conf.containsKey(reportVersionColumnKey)) {
      val infoVersionColumn = conf.getString(reportVersionColumnKey)
      log.info(s"Information version: Derived from the configured column = $infoVersionColumn")
      new InfoVersionColumnFactory(infoVersionColumn)
    } else {
      log.info(s"Info version: default version = 1")
      new InfoVersionLiteralFactory(1)
    }
  }
}
