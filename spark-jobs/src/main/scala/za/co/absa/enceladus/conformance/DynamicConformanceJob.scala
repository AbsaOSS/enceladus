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
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.dao.rest.RestDaoFactory. AvailabilitySetup
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.enceladus.utils.modules.SourcePhase

object DynamicConformanceJob extends ConformanceExecution {
  private val jobName: String = "Enceladus Conformance"

  def main(args: Array[String]) {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    implicit val cmd: ConformanceConfig = ConformanceConfig.getFromArguments(args)

    initialValidation()
    implicit val spark: SparkSession = obtainSparkSession(jobName) // initialize spark
    val restApiCredentials = cmd.restApiCredentialsFactory.getInstance()
    val restApiAvailabilitySetupValue = AvailabilitySetup.withName(restApiAvailabilitySetup)
    implicit val dao: EnceladusDAO = RestDaoFactory.getInstance(
      restApiCredentials,
      restApiBaseUrls,
      restApiUrlsRetryCount,
      restApiAvailabilitySetupValue,
      restApiOptionallyRetryableExceptions)
    implicit val configReader: ConfigReader = new ConfigReader()

    val preparationResult = prepareJob()
    prepareConformance(preparationResult)
    val inputData = readConformanceInputData(preparationResult.pathCfg)

    try {
      val result = conform(inputData, preparationResult)
      processConformanceResult(args, result, preparationResult, restApiCredentials)
      // post processing deliberately rereads the output to make sure that outputted data is stable #1538
      runPostProcessing(SourcePhase.Conformance, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}

