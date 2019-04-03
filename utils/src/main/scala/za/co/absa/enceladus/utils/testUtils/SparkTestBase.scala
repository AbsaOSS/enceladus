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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConversions._
import java.io.File
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

trait SparkTestBase { self =>
  System.setProperty("user.timezone", "UTC");
  TimeZoneNormalizer.normalizeAll(Seq(spark))

  val config = ConfigFactory.load()
  val sparkMaster = config.getString("enceladus.utils.testUtils.sparkTestBaseMaster")

  val sparkBuilder = SparkSession.builder()
    .master(sparkMaster)
    .appName(s"Enceladus test - ${self.getClass.getName}")
    .config("spark.ui.enabled", "false")

  implicit val spark = if (sparkMaster == "yarn") {
    val confDir = config.getString("enceladus.utils.testUtils.hadoop.conf.dir")
    val distJarsDir = config.getString("enceladus.utils.testUtils.spark.distJars.dir")
    val sparkHomeDir = config.getString("enceladus.utils.testUtils.spark.home.dir")
    
    //load all hadoop configs
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path(s"$confDir/hdfs-site.xml"))
    hadoopConf.addResource(new Path(s"$confDir/yarn-site.xml"))
    hadoopConf.addResource(new Path(s"$confDir/core-site.xml"))

    val hadoopConfigs = hadoopConf.iterator().map(entry => (s"spark.hadoop.${entry.getKey}", entry.getValue)).toMap

    //load spark configs from SPARK_HOME
    val sparkConfigIn = ConfigFactory.empty().atPath(s"$sparkHomeDir/conf/spark-defaults.conf")
    val sparkConfigs = sparkConfigIn.entrySet().filter(_.getKey != "spark.yarn.jars").map(entry => (entry.getKey, entry.getValue.unwrapped().toString())).toMap   
    
    val allConfigs = hadoopConfigs ++ sparkConfigs
    
    //get a list of all dist jars
    val distJars = FileSystem.get(hadoopConf).listStatus(new Path(distJarsDir)).map(_.getPath)

    //get a list of jars used by the given test - spark jars are taken from dist jars, so include absa deps only
    val cl = this.getClass.getClassLoader
    val localJars = cl.asInstanceOf[java.net.URLClassLoader].getURLs.filter(c => c.toString().contains("absa")).map(_.toString()) 
    
    //figure out the current jar - we also need to send the class of the test to executors
    val targetDir = new File(s"${System.getProperty("user.dir")}/target")
    val currentJars = targetDir.listFiles().filter(f => f.getName.split("\\.").last.toLowerCase() == "jar" && f.getName.contains("original"))
        
    val deps = (distJars ++ localJars ++ currentJars).mkString(",")

    sparkBuilder.config(new SparkConf().setAll(allConfigs))
      .config("spark.yarn.jars", deps)
      .config("spark.deploy.mode", "client")
      .getOrCreate()

  } else {
    sparkBuilder
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }

  // Do not display INFO entries for tests
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

}
