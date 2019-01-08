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

package za.co.absa.enceladus.kafka.files

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

/**
  * This class sends files, through SparkContext, to be downloaded and used by executors.
  */
object JobFilesDispatcher {

  val log: Logger = LogManager.getLogger(JobFilesDispatcher.getClass)

  /**
    * Sends a list of files to executors.
    * The files are NOT validated, since they can be everywhere (i.e. local FSs, HDFS, network, etc), thus,
    * it is important that the caller makes sure they exist and are reacheable.
    *
    * @param absoluteFilesPaths List of absolute file paths.
    * @param spark Running SparkSession instance.
    */
  def sendFiles(absoluteFilesPaths: List[String], spark: SparkSession): Unit = {

    if (absoluteFilesPaths.nonEmpty) {
      absoluteFilesPaths.foreach(path => {
        spark.sparkContext.addFile(path)
        log.info(s"Added file to be used by executors: $path")
      })
    }
    else {
      log.warn("Method invoked but list of files to be downloaded by workers is empty.")
    }
  }
}
