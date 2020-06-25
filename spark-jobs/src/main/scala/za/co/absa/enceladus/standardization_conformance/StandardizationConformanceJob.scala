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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.plugins.builtin.utils.SecureKafka
import za.co.absa.enceladus.standardization_conformance.config.StdConformanceConfigInstance
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.modules.SourceId
import za.co.absa.enceladus.utils.udf.UDFLibrary

object StandardizationConformanceJob extends StdConformanceExecution {
  def main(args: Array[String]): Unit = {
    SecureKafka.setSecureKafkaProperties(conf)

    SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

    implicit val cmd: StdConformanceConfigInstance = StdConformanceConfigInstance.getCmdLineArguments(args)

    implicit val spark: SparkSession = obtainSparkSession()
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    implicit val udfLib: UDFLibrary = new UDFLibrary
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    val preparationResult = prepareJob()
    val schema =  prepareStandardization(args, menasCredentials, preparationResult)
    prepareConformance(preparationResult)
    val inputData = readStandardizationInputData(schema, cmd, preparationResult.pathCfg.inputPath, preparationResult.dataset)


    try {

      val standardized = standardize(inputData, schema, cmd)
      val processedStandardization = processStandardizationResult(args, standardized, preparationResult, schema, cmd, menasCredentials)
      runPostProcessing(SourceId.Standardization, preparationResult, cmd)

      val result = conform(processedStandardization, preparationResult)

      processConformanceResult(args, result, preparationResult, menasCredentials)
      runPostProcessing(SourceId.Conformance, preparationResult, cmd)
    } finally {
      finishJob(cmd)
    }
  }
}
