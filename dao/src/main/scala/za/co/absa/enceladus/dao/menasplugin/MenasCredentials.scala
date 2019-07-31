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

package za.co.absa.enceladus.dao.menasplugin

import java.io.File

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.typesafe.config.ConfigFactory

import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils

object MenasCredentials {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Retrieves Menas Credentials from file either on local file system or on HDFS
   */
  def fromFile(path: String)(implicit spark: SparkSession): MenasCredentials = {
    val conf = if(path.startsWith("hdfs://")) {
     val file = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration).hdfsRead(path)
     ConfigFactory.parseString(file)
    } else {
     ConfigFactory.parseFile(new File(replaceHome(path)))
    }
    MenasCredentials(conf.getString("username"), conf.getString("password"))
  }

  /**
   * Checks whether the file exists either on HDFS or on the local file system
   */
  def exists(path: String)(implicit spark: SparkSession): Boolean = {
    new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration).exists(path)
  }

  def replaceHome(path: String): String = {
    if (path.matches("^~.*")) {
      //not using replaceFirst as it interprets the backslash in Windows path as escape character mangling the result
      System.getProperty("user.home") + path.substring(1)
    } else {
      path
    }
  }

}

case class MenasCredentials(username: String, password: String)
