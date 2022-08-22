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

package za.co.absa.enceladus.standardization_conformance

import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.common.GlobalDefaults
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.dao.rest.RestDaoFactory.AvailabilitySetup
import za.co.absa.enceladus.standardization_conformance.config.StandardizationConformanceConfig
import za.co.absa.enceladus.utils.config.ConfigReader
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}

object StandardizationAndConformanceJob extends StandardizationAndConformanceExecution {
  private val jobName = "Enceladus Standardization&Conformance"

  def main(args: Array[String]): Unit = {
    implicit val cmd: StandardizationConformanceConfig = StandardizationConformanceConfig.getFromArguments(args)

    initialValidation()
    implicit val spark: SparkSession = obtainSparkSession(jobName)
    implicit val configReader: ConfigReader = new ConfigReader()

    val menasCredentials = cmd.menasCredentialsFactory.getInstance()
    val menasSetupValue = AvailabilitySetup.withName(menasSetup)
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls, menasUrlsRetryCount, menasSetupValue)
    implicit val defaults: GlobalDefaults.type = GlobalDefaults

    val preparationResult = prepareJob()
    val schema = prepareStandardization(args, menasCredentials, preparationResult)
    val stdInputData = readStandardizationInputData(schema, cmd, preparationResult.pathCfg.raw, preparationResult.dataset)

    val metadataColumns = BasicMetadataColumnsConfig.fromDefault().copy(prefix = "enceladus")
    val standardizationConfigWithoutTZ = BasicStandardizationConfig.fromDefault().copy(metadataColumns = metadataColumns)
    val standardizationConfig = configReader.getStringOption("timezone") match {
      case Some(tz) => standardizationConfigWithoutTZ.copy(timezone = tz)
      case None => standardizationConfigWithoutTZ
    }

    try {
      val standardized = standardize(stdInputData, schema, standardizationConfig)
      processStandardizationResult(args, standardized, preparationResult, schema, cmd, menasCredentials)
      // post processing deliberately rereads the output to make sure that outputted data is stable #1538
      runPostProcessing(SourcePhase.Standardization, preparationResult, cmd)
      standardized.unpersist()

      prepareConformance(preparationResult)
      val confInputData = readConformanceInputData(preparationResult.pathCfg)
      val result = conform(confInputData, preparationResult)
      processConformanceResult(args, result, preparationResult, menasCredentials)

      // post processing deliberately rereads the output ... same as above
      runPostProcessing(SourcePhase.Conformance, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}
