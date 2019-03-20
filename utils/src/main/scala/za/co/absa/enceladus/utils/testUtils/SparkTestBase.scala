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
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

trait SparkTestBase {
  System.setProperty("user.timezone", "UTC");

  implicit val spark = SparkSession.builder().master("local[*]").appName("test")
  .config("spark.sql.codegen.wholeStage", false)
  .config("spark.ui.enabled", "false")
  .config("spark.driver.bindAddress","127.0.0.1")
  .config("spark.driver.host", "127.0.0.1")
  .getOrCreate()
  TimeZoneNormalizer.normalizeAll(Seq(spark))

  // Do not display INFO entries for tests
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

}
