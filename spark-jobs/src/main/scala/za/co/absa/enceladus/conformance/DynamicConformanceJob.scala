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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.common.JobCmdConfig
import za.co.absa.enceladus.common.RecordIdGeneration._
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams.ErrorSourceId
import za.co.absa.enceladus.plugins.builtin.utils.SecureKafka
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils

object DynamicConformanceJob extends ConformanceExecution {

  def main(args: Array[String]) {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    SecureKafka.setSecureKafkaProperties(conf)

    SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

    implicit val cmd: ConfCmdConfig = ConfCmdConfig.getCmdLineArguments(args)
    implicit val jobCmdConfig: JobCmdConfig = cmd.jobConfig
    implicit val spark: SparkSession = obtainSparkSession() // initialize spark
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    val menasCredentials = cmd.jobConfig.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    dao.authenticate()
    // get the dataset definition
    val dataset = dao.getDataset(jobCmdConfig.datasetName, jobCmdConfig.datasetVersion)
    val reportVersion = getReportVersion(cmd.jobConfig, dataset)
    val pathCfg = getPathCfg(cmd, dataset, reportVersion)
    val recordIdGenerationStrategy = getRecordIdGenerationStrategyFromConfig(conf)

    log.info(s"stdpath = ${pathCfg.inputPath}")
    log.info(s"publishPath = ${pathCfg.outputPath}")
    // die before performing any computation if the output path aleady exists
    validateForExistingOutputPath(fsUtils, pathCfg)

    initFunctionalExtensions(reportVersion, pathCfg)
    val performance = initPerformanceMeasurer(pathCfg.inputPath)

    // load data for input and mapping tables
    val inputData = spark.read.parquet(pathCfg.inputPath)

    try {
      val result = conform(dataset, inputData, recordIdGenerationStrategy)

      processConformanceResult(result, performance, pathCfg, reportVersion, menasCredentials)
      log.info(s"$step finished successfully")

      runPostProcessors(ErrorSourceId.Conformance, pathCfg, jobCmdConfig, reportVersion)
    } finally {
      executePostStep(jobCmdConfig)
    }
  }
}
