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

package za.co.absa.enceladus.menas

import org.springframework.context.annotation.{ Configuration, Bean }
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

@Configuration
class SparkConfig {

  @Value("${menas.spark.master}")
  val master: String = ""

  @Bean
  def spark: SparkSession = {
    val sparkSession = SparkSession.builder()
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName("Menas Spark controller")
      .getOrCreate()
    TimeZoneNormalizer.normalizeAll(sparkSession)
    sparkSession
  }

}
