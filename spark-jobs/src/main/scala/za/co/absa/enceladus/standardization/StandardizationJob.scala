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

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.fs.DistributedFsUtils
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.udf.UDFLibrary

object StandardizationJob extends StandardizationExecution {
  private val jobName: String = "Enceladus Standardization"

  def main(args: Array[String]) {
    initialValidation()

    implicit val cmd: StandardizationConfig = StandardizationConfig.getFromArguments(args)
    implicit val spark: SparkSession = obtainSparkSession(jobName)

    implicit val udfLib: UDFLibrary = new UDFLibrary
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    val preparationResult = prepareJob()
    val schema =  prepareStandardization(args, menasCredentials, preparationResult)
    val inputData = readStandardizationInputData(schema, cmd, preparationResult.pathCfg.raw, preparationResult.dataset)

    try {
      val result = standardize(inputData, schema, cmd)
      processStandardizationResult(args, result, preparationResult, schema, cmd, menasCredentials)
      runPostProcessing(SourcePhase.Standardization, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}
