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

package za.co.absa.enceladus.rest_api

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class EnceladusFsConfig @Autowired()(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  @Value("${rest_api.fs.config.type}")
  val fsType: String = ""

  @Bean
  def hadoopFS(): EnceladusFileSystem = {
    val fsTypeEnum = EnceladusFsType.withName(fsType.toLowerCase)
    fsTypeEnum match {
      case EnceladusFsType.Hdfs =>
        logger.info(s"Using FS config for HDFS.")
        EnceladusFileSystem(HDFSConfig.hadoopFS()(spark))

      case EnceladusFsType.NoFs =>
        logger.info(s"Not using any FS config.")
        EnceladusFileSystem.empty

      case _ =>
        throw new Exception("Unsupported FileSystem")
    }
  }
}

object EnceladusFsType extends Enumeration {
  val NoFs = Value("none")
  val Hdfs = Value("hdfs")
}
