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
    * Adds performance metrics to the Spark job metadata. Atum is used to set these metrics.
    *
    * @param spark            A Spark Session
    * @param optionPrefix     A prefix for all performance metrics options, e.g. 'std' for Standardization and 'conform' for Conformance
    * @param inputPath        A path to an input directory of the job
    * @param outputPath       A path to an output directory of the job
    * @param loginUserName    A login user name who performed the job
    *
    **/
  def addPerformanceMetricsToAtumMetadata(spark: SparkSession,
                                          optionPrefix: String,
                                          inputPath: String,
                                          outputPath: String,
                                          loginUserName: String
                                         ): Unit = {
    // Spark job configuration
    val sc = spark.sparkContext
    sc.getConf.getAll.mkString("\n")

    // The number of executors minus the driver
    val numberOfExecutrs = sc.getExecutorMemoryStatus.keys.size - 1
    val executorMemory = spark.sparkContext.getConf.get("spark.executor.memory")

    // Directory sizes and size ratio
    val inputDirSize = FileSystemVersionUtils.getDirectorySize(inputPath)(spark)
    val outputDirSize = FileSystemVersionUtils.getDirectorySize(outputPath)(spark)

    val (numRecordsFailed, numRecordsSuccessful, numOfErrors) = getNumberOfErrors(spark, outputPath)

    if (doesSizeRatioMakesSense(inputDirSize, numRecordsFailed + numRecordsSuccessful)) {
      val percent = (outputDirSize.toDouble / inputDirSize.toDouble) * 100
      val percentFormatted = f"$percent%3.2f"
      Atum.setAdditionalInfo(s"${optionPrefix}_size_ratio" -> s"$percentFormatted %")
    }

    Atum.setAdditionalInfo(s"${optionPrefix}_input_dir_size" -> inputDirSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_output_dir_size" -> outputDirSize.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_enceladus_version" -> ProjectMetadataTools.getEnceladusVersion)
    Atum.setAdditionalInfo(s"${optionPrefix}_application_id" -> spark.sparkContext.applicationId)
    Atum.setAdditionalInfo(s"${optionPrefix}_username" -> loginUserName)
    Atum.setAdditionalInfo(s"${optionPrefix}_executors_num" -> s"$numberOfExecutrs")
    Atum.setAdditionalInfo(s"${optionPrefix}_executors_memory" -> s"$executorMemory")
    Atum.setAdditionalInfo(s"${optionPrefix}_records_succeeded" -> numRecordsSuccessful.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_records_failed" -> numRecordsFailed.toString)
    Atum.setAdditionalInfo(s"${optionPrefix}_errors_count" -> numOfErrors.toString)

    if (numRecordsSuccessful == 0) {
      logger.error("No successful records after running the Spark Application. Possibly the schema is incorrectly " +
        "defined for the dataset.")
    }
  }

  /** Returns if output to input size ratio makes sense. The input dir size should be begger than zero and the output
    * dataframe should contain at leat one record
    *  */
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
