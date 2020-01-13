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

package za.co.absa.enceladus.utils.testUtils

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.event.Level
import org.slf4j.event.Level._

trait LoggerTestBase {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def logLevelToLogFunction(logLevel: Level): String => Unit = {
    logLevel match {
      case TRACE => logger.trace
      case DEBUG => logger.debug
      case INFO  => logger.info
      case WARN  => logger.warn
      case ERROR => logger.error
    }
  }

  protected def logDataFrameContent(df: DataFrame, logLevel: Level = DEBUG): Unit = {
    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

    val logFnc = logLevelToLogFunction(logLevel)
    logFnc(df.schema.treeString)

    val dfData = df.dataAsString(false)
    logFnc(dfData)
  }
}
