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

package za.co.absa.enceladus.plugins.builtin.errorinfo.mq

import java.time.Instant

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, explode, lit, size, struct}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders}
import za.co.absa.enceladus.plugins.api.postprocessor.{PostProcessor, PostProcessorPluginParams}
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin.SingleErrorStardardized
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.kafka.KafkaErrorInfoPlugin
import za.co.absa.enceladus.utils.schema.SchemaUtils
import ErrorInfoSenderPlugin._
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams.ErrorSourceId
import za.co.absa.enceladus.utils.error.ErrorMessage.ErrorCodes


class ErrorInfoSenderPlugin(connectionParams: KafkaConnectionParams,
                            keySchemaRegistryConfig: Map[String, String],
                            valueSchemaRegistryConfig: Map[String, String]) extends PostProcessor {

  private val log = LogManager.getLogger(classOf[ErrorInfoSenderPlugin])

  override def close(): Unit = {}

  /**
   * When data is ready, the error info record(s) are pusblished to kafka.
   *
   * @param dataFrame error data only.
   * @param params    Additional key/value parameters provided by Enceladus.
   * @return A dataframe with post processing applied
   */
  override def onDataReady(dataFrame: DataFrame, params: PostProcessorPluginParams): DataFrame = {
    if (!SchemaUtils.fieldExists(ColumnNames.enceladusRecordId, dataFrame.schema)) {
      throw new IllegalStateException(
        s"${this.getClass.getName} requires ${ColumnNames.enceladusRecordId} column to be present in the dataframe!"
      )
    }

    val dfWithErrors = getIndividualErrors(dataFrame, params)
    sendErrorsToKafka(dfWithErrors, params)
    dfWithErrors
  }

  /**
   * Processes the `dataFrame` - errors are exploded (one error = one line) and filtered to conform to the error source (standardization/conformance)
   * @param dataFrame
   * @param params
   * @return
   */
  def getIndividualErrors(dataFrame: DataFrame, params: PostProcessorPluginParams): DataFrame = {
    implicit val singleErrorStardardizedEncoder = Encoders.product[SingleErrorStardardized]
    implicit val dceErrorInfoEncoder = Encoders.product[DceErrorInfo]

    val allowedErrorCodes = ErrorInfoSenderPlugin.errorCodesForSource(params.sourceId)

    val stdErrors = dataFrame
      .filter(size(col("errCol")) > 0)
      // only keep columns that are needed for the actual error publishing
      .select(
        col(ColumnNames.enceladusRecordId).cast(DataTypes.StringType).as("recordId"),
        col(ColumnNames.reportDate),
        explode(col(ColumnNames.errCol)).as("singleError")
      )
      .as[SingleErrorStardardized]
      .map(_.toErrorInfo(params))
      .filter(entry => allowedErrorCodes.contains(entry.errorCode)) // Std xor Conf error codes
      .toDF()

    stdErrors
  }

  def sendErrorsToKafka(stdErrors: DataFrame, params: PostProcessorPluginParams): Unit = {
    log.info(s"Sending errors to kafka topic ${connectionParams.topicName} ...")

    val valueAvroSchemaString = KafkaErrorInfoPlugin.getValueAvroSchemaString
    val valueSchemaType = KafkaErrorInfoPlugin.getValueStructTypeSchema

    val keyAvroSchemaString = KafkaErrorInfoPlugin.getKeyAvroSchemaString

    val allValueColumns = struct(stdErrors.columns.head, stdErrors.columns.tail: _*)
    import za.co.absa.abris.avro.functions.to_confluent_avro

    stdErrors.sqlContext.createDataFrame(stdErrors.rdd, valueSchemaType) // forces avsc schema to assure compatible nullability of the DF
      .select(
        to_confluent_avro(
          struct(lit(params.sourceSystem).as("sourceSystem")), keyAvroSchemaString, keySchemaRegistryConfig
        ).as("key"),
        to_confluent_avro(allValueColumns, valueAvroSchemaString, valueSchemaRegistryConfig).as("value")
      )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", connectionParams.bootstrapServers)
      .option("topic", connectionParams.topicName)
      .option("kafka.client.id", connectionParams.clientId)
      .option("path", "notReallyUsedButAtumExpectsItToBePresent") // TODO Atum issue #32
      .save()

  }
}

object ErrorInfoSenderPlugin {

  // columns from the original datafram (post Stdardardization/Conformance) to be addressed
  object ColumnNames {
    val enceladusRecordId = "enceladus_record_id"
    val reportDate = "reportDate"
    val errCol = "errCol"
  }

  case class ErrorRecord(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

  case class SingleErrorStardardized(recordId: String, reportDate: java.sql.Date, singleError: ErrorRecord) {
    def toErrorInfo(additionalParams: PostProcessorPluginParams): DceErrorInfo = DceErrorInfo(
      sourceSystem = additionalParams.sourceSystem,
      sourceSystemId = None,
      dataset = Some(additionalParams.datasetName),
      ingestionNumber = None,
      processingTimestamp = Instant.now.toEpochMilli,
      informationDate = Some(reportDate.toLocalDate.toEpochDay.toInt),
      outputFileName = Some(additionalParams.outputPath),
      recordId = recordId,
      errorSourceId = additionalParams.sourceId.toString,
      errorType = singleError.errType,
      errorCode = singleError.errCode,
      errorDescription = singleError.errMsg,
      additionalInfo = Map(
        "reportDate" -> additionalParams.reportDate,
        "reportVersion" -> additionalParams.reportVersion.toString,
        "datasetName" -> additionalParams.datasetName,
        "datasetVersion" -> additionalParams.datasetVersion.toString
      ) ++ additionalParams.uniqueRunId.fold(Map.empty[String, String])(runId => Map("uniqueRunId" -> runId))
        ++ additionalParams.runId.fold(Map.empty[String, String])(runId => Map("runId" -> runId.toString))
        ++ additionalParams.runUrls.fold(Map.empty[String, String])(runUrls => Map("runUrl" -> runUrls))
    )
  }

  def errorCodesForSource(sourceId: ErrorSourceId.Value): Seq[String] = sourceId match {
    case ErrorSourceId.Standardization => ErrorCodes.standardizationErrorCodes
    case ErrorSourceId.Conformance => ErrorCodes.conformanceErrorCodes
  }

}
