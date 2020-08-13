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

package za.co.absa.enceladus.conformance

import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.modules.SourcePhase

object DynamicConformanceJob extends ConformanceExecution {
  private val jobName: String = "Enceladus Conformance"

  def main(args: Array[String]) {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    initialValidation()

    implicit val cmd: ConformanceConfig = ConformanceConfig.getFromArguments(args)
    implicit val spark: SparkSession = obtainSparkSession(jobName) // initialize spark
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    val preparationResult = prepareJob()
    prepareConformance(preparationResult)
    val inputData = readConformanceInputData(preparationResult.pathCfg)

    try {
      val result = conform(inputData, preparationResult)
      processConformanceResult(args, result, preparationResult, menasCredentials)
      runPostProcessing(SourcePhase.Conformance, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}

