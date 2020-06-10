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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.common._
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.rest.RestDaoFactory
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams.ErrorSourceId
import za.co.absa.enceladus.plugins.builtin.utils.SecureKafka
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.performance.PerformanceMetricTools
import za.co.absa.enceladus.utils.udf.UDFLibrary

object StandardizationJob extends StandardizationExecution {

  def main(args: Array[String]) {
    // This should be the first thing the app does to make secure Kafka work with our CA.
    // After Spring activates JavaX, it will be too late.
    SecureKafka.setSecureKafkaProperties(conf)

    SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

    implicit val cmd: StdCmdConfig = StdCmdConfig.getCmdLineArguments(args)
    implicit val jobCmdConfig: JobCmdConfig = cmd.jobConfig
    implicit val spark: SparkSession = obtainSparkSession()
    implicit val fsUtils: FileSystemVersionUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    implicit val udfLib: UDFLibrary = new UDFLibrary
    val menasCredentials = cmd.jobConfig.menasCredentialsFactory.getInstance()
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    dao.authenticate()

    val dataset = dao.getDataset(jobCmdConfig.datasetName, jobCmdConfig.datasetVersion)
    val reportVersion = getReportVersion(cmd.jobConfig, dataset)
    val pathCfg = getPathCfg(cmd, dataset, reportVersion)

    log.info(s"input path: ${pathCfg.inputPath}")
    log.info(s"output path: ${pathCfg.outputPath}")
    // die if the output path exists
    validateForExistingOutputPath(fsUtils, pathCfg)

    initFunctionalExtensions(reportVersion, pathCfg, true, true)

    // Add report date and version (aka Enceladus info date and version) to Atum's metadata
    Atum.setAdditionalInfo(Constants.InfoDateColumn -> cmd.jobConfig.reportDate)
    Atum.setAdditionalInfo(Constants.InfoVersionColumn -> reportVersion.toString)

    // Add the raw format of the input file(s) to Atum's metadata as well
    Atum.setAdditionalInfo("raw_format" -> cmd.rawFormat)
    val schema: StructType = dao.getSchema(dataset.schemaName, dataset.schemaVersion)
    val dfAll: DataFrame = prepareDataFrame(schema, cmd, pathCfg.inputPath, dataset)

    val performance = initPerformanceMeasurer(pathCfg.inputPath)
    PerformanceMetricTools.addJobInfoToAtumMetadata("std", pathCfg.inputPath, pathCfg.outputPath,
      menasCredentials.username, cmd.jobConfig.args.mkString(" "))

    try {
      val result = standardize(dfAll, schema, cmd)

      processStandardizationResult(result, performance, pathCfg, schema, cmd, menasCredentials)
      log.info(s"$step finished successfully")

      runPostProcessors(ErrorSourceId.Standardization, pathCfg, jobCmdConfig, reportVersion)
    } finally {
      executePostStep(jobCmdConfig)
    }
  }
}
