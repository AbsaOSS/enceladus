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

import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer
import za.co.absa.spark.commons.test.{SparkTestBase, SparkTestConfig}

trait TZNormalizedSparkTestBase extends SparkTestBase {
  override protected def initSpark(implicit sparkConfig: SparkTestConfig): SparkSession = {
    val result = super.initSpark

    //TODO make conditional on empty SparkTestBase.timezone, once SparkCommons 0.3.0 will have been released
    TimeZoneNormalizer.normalizeAll(result)

    result
  }
}
