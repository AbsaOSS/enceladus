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

package za.co.absa.enceladus.migrations.services

import java.io.InputStream

import org.slf4j.LoggerFactory
import za.co.absa.enceladus.migrations.exceptions.BackupException
import za.co.absa.enceladus.migrations.models.BackupConfiguration

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

abstract class BackupService() {

  private[services] val DUMP = "mongodump"
  private[services] val RESTORE = "mongorestore"

  private[services] val log = LoggerFactory.getLogger(this.getClass)
  private[services] val mongoLog = LoggerFactory.getLogger("process.mongo")

  def dump(backupConf: BackupConfiguration): Unit
  def restore(backupConf: BackupConfiguration): Unit

  private[services] def runProcess(processName: String, cmdArgs: List[String]): Unit = {
    val builder = new ProcessBuilder((processName :: cmdArgs).asJava)
    log.info(s"Running command: $processName ${cmdArgs.mkString(" ")}")
    val process = builder.start

    redirectOutputToLogger(process)
    process.waitFor

    val exitValue = process.exitValue
    if (exitValue != 0) {
      throw new BackupException(s"Process $processName exited with code: $exitValue")
    }
  }

  private def redirectOutputToLogger(process: Process): Unit = {
    redirectStream(process.getErrorStream) // all mongo logs go to STDERR and ignore STDOUT
    redirectStream(process.getInputStream)
  }

  private def redirectStream(stream: InputStream) = {
    Future {
      Source.fromInputStream(stream).getLines().foreach(mongoLog.info)
    }
  }
}
