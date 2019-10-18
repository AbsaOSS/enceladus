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

package za.co.absa.enceladus.utils.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, sum}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.core.Atum
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.general.ProjectMetadataTools
import za.co.absa.enceladus.utils.schema.SchemaUtils

object PerformanceMetricTools {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Adds general job information to Atum's metadata that will end up in an info file.
    *
    * The method should be ran at the beginning of a job so that this general information is available for debugging.
    *
    * @param spark         A Spark Session
    * @param optionPrefix  A prefix for all performance metrics options, e.g. 'std' for Standardization and 'conform' for Conformance
    * @param inputPath     A path to an input directory of the job
    * @param outputPath    A path to an output directory of the job
    * @param loginUserName A login user name who performed the job
    *
    **/
  def addJobInfoToAtumMetadata(optionPrefix: String,
                                inputPath: String,
                                outputPath: String,
                                loginUserName: String,
                                cmdLineArgs: String)
                               (implicit spark: SparkSession): Unit = {
    // Spark job configuration
    val sc = spark.sparkContext

    // The number of executors minus the driver
    val numberOfExecutors = sc.getExecutorMemoryStatus.keys.size - 1

    val fsUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)
    // Directory sizes and size ratio
    val inputDirSize = fsUtils.getDirectorySize(inputPath)
    val inputDataSize = fsUtils.getDirectorySizeNoHidden(inputPath)

    addSparkConfig(optionPrefix, "spark.driver.memory", "driver_memory")
    addSparkConfig(optionPrefix, "spark.driver.cores", "driver_cores")
    addSparkConfig(optionPrefix, "spark.driver.memoryOverhead", "driver_memory_overhead")
    addSparkConfig(optionPrefix, "spark.executor.memory", "executor_memory")
    addSparkConfig(optionPrefix, "spark.executor.cores", "executor_cores")
    addSparkConfig(optionPrefix, "spark.executor.memoryOverhead", "executor_memory_overhead")
    addSparkConfig(optionPrefix, "spark.submit.deployMode", "yarn_deploy_mode")
    addSparkConfig(optionPrefix, "spark.master", "spark_master")

    Atum.setAdditionalInfo(s"${optionPrefix}_cmd_line_args" -> cmdLineArgs)
    Atum.setAdditionalInfo(s"${optionPrefix}_input_dir" -> inputPath)
    Atum.setAdditionalInfo(s"${optionPrefix}_output_dir" -> outputPath)
    Atum.setAdditionalInfo(s"${optionPrefix}_input_dir_size" -> inputDirSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_input_data_size" -> inputDataSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_enceladus_version" -> ProjectMetadataTools.getEnceladusVersion)
    Atum.setAdditionalInfo(s"${optionPrefix}_application_id" -> spark.sparkContext.applicationId)
    Atum.setAdditionalInfo(s"${optionPrefix}_username" -> loginUserName)
    Atum.setAdditionalInfo(s"${optionPrefix}_executors_num" -> s"$numberOfExecutors")
  }


  /**
    * Adds performance metrics to the Spark job metadata. Atum is used to set these metrics.
    *
    * @param spark         A Spark Session
    * @param optionPrefix  A prefix for all performance metrics options, e.g. 'std' for Standardization and 'conform' for Conformance
    * @param inputPath     A path to an input directory of the job
    * @param outputPath    A path to an output directory of the job
    * @param loginUserName A login user name who performed the job
    *
    **/
  def addPerformanceMetricsToAtumMetadata(spark: SparkSession,
                                          optionPrefix: String,
                                          inputPath: String,
                                          outputPath: String,
                                          loginUserName: String,
                                          cmdLineArgs: String
                                         ): Unit = {
    val fsUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

    // Directory sizes and size ratio
    val inputDirSize = fsUtils.getDirectorySize(inputPath)
    val inputDataSize = fsUtils.getDirectorySizeNoHidden(inputPath)
    val outputDirSize = fsUtils.getDirectorySize(outputPath)
    val outputDataSize = fsUtils.getDirectorySizeNoHidden(outputPath)

    val (numRecordsFailed, numRecordsSuccessful, numOfErrors) = getNumberOfErrors(spark, outputPath)

    calculateSizeRatio(inputDirSize, outputDataSize, numRecordsFailed + numRecordsSuccessful)
      .foreach(ratio => {
        Atum.setAdditionalInfo(s"${optionPrefix}_size_ratio" -> prettyPercent(ratio * 100))
      })

    calculateSizeRatio(inputDirSize, outputDataSize, numRecordsFailed + numRecordsSuccessful)
      .foreach(ratio => {
        Atum.setAdditionalInfo(s"${optionPrefix}_data_size_ratio" -> prettyPercent(ratio * 100))
      })

    Atum.setAdditionalInfo(s"${optionPrefix}_output_dir_size" -> outputDirSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_output_data_size" -> outputDataSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_record_count" -> (numRecordsSuccessful + numRecordsFailed).toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_records_succeeded" -> numRecordsSuccessful.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_records_failed" -> numRecordsFailed.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_errors_count" -> numOfErrors.toString)

    if (numRecordsSuccessful == 0) {
      logger.error("No successful records after running the Spark Application. Possibly the schema is incorrectly " +
        "defined for the dataset.")
    }
  }

  /**
    * Format a percentages in a pretty way.
    *
    * @param percent A percentage value.
    * @return A pretty formatted percentage value.
    */
  private def prettyPercent(percent: Double): String = f"$percent%3.2f %%"

  /**
    * Adds a Spark config key-value to Atum metadata if such key is present in Spark runtime config.
    *
    * @param optionPrefix A prefix for a job (e.g. "std", "conf", etc.)
    * @param sparkKey     A Spark configuration key
    * @param atumKey      An Atum metadata key
    */
  private def addSparkConfig(optionPrefix: String, sparkKey: String, atumKey: String)
                            (implicit spark: SparkSession): Unit = {
    val sparkConfigValOpt = spark.sparkContext.getConf.getOption(sparkKey)
    sparkConfigValOpt.foreach(sparkVal => Atum.setAdditionalInfo(s"${optionPrefix}_$atumKey" -> s"$sparkVal"))
  }

  /**
    * Calculates ratio between input and output directory sizes if it makes sense.
    *
    * @param inputDirSize  An input directory size in bytes
    * @param outputDirSize An output directory size in bytes
    * @param numRecords    Number of records in an output dataset
    * @return A ratio between input and output directory sizes if it makes sense, None otherwise
    */
  private def calculateSizeRatio(inputDirSize: Long,
                                 outputDirSize: Long,
                                 numRecords: Long): Option[Double] = {
    if (doesSizeRatioMakesSense(inputDirSize, numRecords)) {
      Option(outputDirSize.toDouble / inputDirSize.toDouble)
    } else {
      None
    }
  }

  /**
    * Returns if output to input size ratio makes sense. The input dir size should be bigger than zero and the output
    * dataset should contain at least one record
    *
    * @param inputDirSize An input directory size in bytes
    * @param numRecords   Number of records in an output dataset
    * @return true if it makes sense to calculate input to output size ratio.
    */
  private def doesSizeRatioMakesSense(inputDirSize: Long,
                                      numRecords: Long): Boolean = {
    inputDirSize > 0 && numRecords > 0
  }

  /** Returns the number of records failed, the number of records succeeded and the total number of errors encountered
    * when running a Standardization or a Dynamic Conformance job. */
  private def getNumberOfErrors(spark: SparkSession, outputPath: String): (Long, Long, Long) = {
    val df = spark.read.parquet(outputPath)
    val errorCountColumn = SchemaUtils.getClosestUniqueName("enceladus_error_count", df.schema)
    val errCol = col(ErrorMessage.errorColumnName)
    val numRecordsFailed = df.filter(size(errCol) > 0).count
    val numRecordsSuccessful = df.filter(size(errCol) === 0).count

    val numOfErrors = if (numRecordsFailed + numRecordsSuccessful > 0) {
      df.withColumn(errorCountColumn, size(errCol)).agg(sum(col(errorCountColumn)))
        .take(1)(0)(0).toString.toLong
    } else {
      // There are 0 errors in the error column if the output dataframe is empty
      0
    }

    (numRecordsFailed, numRecordsSuccessful, numOfErrors)
  }
}
