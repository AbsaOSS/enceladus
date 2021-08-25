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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}
import za.co.absa.enceladus.conformance.streaming.InfoDateFactory.log

sealed trait InfoVersionFactory {
  def getInfoVersionColumn(): Column
}

class InfoVersionLiteralFactory(reportVersion: Int) extends InfoVersionFactory {
  override def getInfoVersionColumn(): Column = lit(reportVersion)
}

class InfoVersionColumnFactory(columnName: String) extends InfoVersionFactory {
  override def getInfoVersionColumn(): Column = col(columnName)
}

object InfoVersionFactory {
  import za.co.absa.enceladus.conformance.HyperConformanceAttributes._

  def getFactoryFromConfig(conf: Configuration): InfoVersionFactory = {
    if (conf.containsKey(reportVersionKey)) {
      val reportVersion = conf.getInt(reportVersionKey)
      log.info(s"Info version: configuration reportVersion = $reportVersion")
      new InfoVersionLiteralFactory(reportVersion)
    } else if (conf.containsKey(reportVersionColumnKey)) {
      val infoVersionColumn = conf.getString(reportVersionColumnKey)
      log.info(s"Info version: configuration infoVersionColumn = $infoVersionColumn")
      new InfoVersionColumnFactory(infoVersionColumn)
    } else {
      throw new IllegalArgumentException("Neither report version, nor info version is provided.")
    }
  }
}
