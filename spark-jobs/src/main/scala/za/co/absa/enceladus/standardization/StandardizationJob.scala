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

package za.co.absa.enceladus.standardization

import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.dao.rest.RestDaoFactory.AvailabilitySetup
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.types.{Defaults, DefaultsByFormat}
import za.co.absa.enceladus.utils.udf.UDFLibrary

object StandardizationJob extends StandardizationExecution {
  private val jobName: String = "Enceladus Standardization"

  def main(args: Array[String]) {
    implicit val cmd: StandardizationConfig = StandardizationConfig.getFromArguments(args)

    initialValidation()
    implicit val spark: SparkSession = obtainSparkSession(jobName)
    implicit val udfLib: UDFLibrary = new UDFLibrary
    implicit val defaults: Defaults = new DefaultsByFormat(cmd.rawFormat)
    implicit val configReader: ConfigReader = new ConfigReader()

    val restApiCredentials = cmd.restApiCredentialsFactory.getInstance()
    val restApiAvailabilitySetupValue = AvailabilitySetup.withName(restApiAvailabilitySetup)
    implicit val dao: MenasDAO = RestDaoFactory
      .getInstance(restApiCredentials, restApiBaseUrls, restApiUrlsRetryCount, restApiAvailabilitySetupValue)

    val preparationResult = prepareJob()
    val schema =  prepareStandardization(args, restApiCredentials, preparationResult)
    val inputData = readStandardizationInputData(schema, cmd, preparationResult.pathCfg.raw, preparationResult.dataset)

    try {
      val result = standardize(inputData, schema, cmd)
      processStandardizationResult(args, result, preparationResult, schema, cmd, restApiCredentials)
      // post processing deliberately rereads the output to make sure that outputted data is stable #1538
      runPostProcessing(SourcePhase.Standardization, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}
