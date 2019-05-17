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

package za.co.absa.enceladus.menasplugin

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object MenasCredentials {

  private def getFS(conf: Configuration) = FileSystem.get(conf)
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  def fromFile(path: String)(implicit spark: SparkSession): MenasCredentials = {
    val conf = if(path.startsWith("hdfs://")) {
      val in = getFS(spark.sparkContext.hadoopConfiguration).open(new Path(path))
      val content = Array.fill(in.available())(0.toByte)
      in.readFully(content)
      val file = new String(content, "UTF-8")
      logger.info(s"File: $file")
     ConfigFactory.parseString(file)
    } else {
     ConfigFactory.parseFile(new File(replaceHome(path)))
    }
    MenasCredentials(conf.getString("username"), conf.getString("password"))
  }
  
  def exists(path: String)(implicit spark: SparkSession): Boolean = {
    if(path.startsWith("hdfs://")) {
      val exists = getFS(spark.sparkContext.hadoopConfiguration).exists(new Path(path))
      logger.info(s"Exists $path $exists")
      exists
    } else {
      new File(path).exists()
    }
  }

  def replaceHome(path: String): String = {
    if (path.matches("^~.*")) {
      //not using replaceFirst as it interprets the backslash in Windows path as escape character mangling the result
      System.getProperty("user.home")+path.substring(1)
    } else {
      path
    }
  }

}

case class MenasCredentials(username: String, password: String)
