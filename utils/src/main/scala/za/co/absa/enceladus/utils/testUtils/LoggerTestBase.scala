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

package za.co.absa.enceladus.utils.testUtils

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import LogLevel._

trait LoggerTestBase {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def logLevelToLogFunction(logLevel: LogLevel): String => Unit = {
    logLevel match {
      case Trace => logger.trace
      case Debug => logger.debug
      case Info  => logger.info
      case Warn  => logger.warn
      case Error => logger.error
    }
  }

  protected def logDataFrameContent(df: DataFrame, logLevel: LogLevel = Debug): Unit = {
    val logFnc = logLevelToLogFunction(logLevel)
    logFnc(df.schema.treeString)

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(truncate = false)
    }
    val dfData = new String(outCapture.toByteArray).replace("\r\n", "\n")
    logFnc(dfData)
  }
}

sealed trait LogLevel

object LogLevel {
  case object Trace extends LogLevel
  case object Debug extends LogLevel
  case object Info extends LogLevel
  case object Warn extends LogLevel
  case object Error extends LogLevel
}
