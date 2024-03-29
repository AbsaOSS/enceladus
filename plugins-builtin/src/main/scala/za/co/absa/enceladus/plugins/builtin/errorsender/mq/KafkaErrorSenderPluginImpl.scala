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

package za.co.absa.enceladus.plugins.builtin.errorsender.mq

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, explode, lit, size, struct, typedLit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Encoder, Encoders}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessor
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.{KafkaConnectionParams, KafkaSecurityParams, SchemaRegistrySecurityParams}
import za.co.absa.enceladus.plugins.builtin.errorsender.DceError
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.KafkaErrorSenderPluginImpl.SingleErrorStardardized
import KafkaErrorSenderPluginImpl._
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.kafka.KafkaErrorSenderPlugin
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams
import za.co.absa.enceladus.utils.error.ErrorMessage.ErrorCodes
import za.co.absa.enceladus.utils.modules._
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.kafka.KafkaErrorSenderPlugin.{avroKeySchemaRegistryConfig, avroValueSchemaRegistryConfig, registerSchemas}

import scala.util.{Failure, Success, Try}


case class KafkaErrorSenderPluginImpl(connectionParams: KafkaConnectionParams) extends PostProcessor {

  private val log = LogManager.getLogger(classOf[KafkaErrorSenderPluginImpl])

  override def close(): Unit = {}

  /**
   * When data is ready, the error record(s) are pusblished to kafka.
   *
   * @param dataFrame error data only.
   * @param paramsMap Additional key/value parameters provided by Enceladus.
   */
  override def onDataReady(dataFrame: DataFrame, paramsMap: Map[String, String]): Unit = {
    if (!dataFrame.schema.fieldExists(ColumnNames.enceladusRecordId)) {
      throw new IllegalStateException(
        s"${this.getClass.getName} requires ${ColumnNames.enceladusRecordId} column to be present in the dataframe!"
      )
    }

    val errorSenderParams = Try(ErrorSenderPluginParams.fromMap(paramsMap)) match {
      case Success(params) => params
      case Failure(e) =>throw new IllegalArgumentException(
        s"Incompatible parameter map supplied for ${KafkaErrorSenderPluginImpl.getClass.getName}: $paramsMap", e
      )
    }

    val dfWithErrors = getIndividualErrors(dataFrame, errorSenderParams)

    if (dfWithErrors.isEmpty) {
      log.info("No errors to be sent to kafka.")
    } else {
      val count = dfWithErrors.count()
      log.info(s"Sending $count errors to kafka topic ${connectionParams.topicName} ...")

      Try {
        val (keySchemaId, valueSchemaId, schemaRegistryParams) = registerSchemas(connectionParams)

        val valueConfig = avroValueSchemaRegistryConfig(schemaRegistryParams, valueSchemaId)
        val keyConfig = avroKeySchemaRegistryConfig(schemaRegistryParams, keySchemaId)

        val forKafkaDf = prepareDataForKafka(dfWithErrors, errorSenderParams, keyConfig, valueConfig)
        sendErrorsToKafka(forKafkaDf)
      } match {
        case Success(_) => log.info(s"$count errors successfully sent to kafka topic ${connectionParams.topicName}")
        case Failure(e) =>
          log.error("Sending errors to kafka unsuccessful due to: ", e)
      }
    }
  }

  /**
   * Processes the `dataFrame` - errors are exploded (one error = one line) and filtered to conform to the error source (standardization/conformance)
   * @param dataFrame standardization/conformance output
   * @param params plugin processing parameters
   * @return DF with exploded errors and corresponding to the given error source
   */
  def getIndividualErrors(dataFrame: DataFrame, params: ErrorSenderPluginParams): DataFrame = {
    implicit val singleErrorStardardizedEncoder: Encoder[SingleErrorStardardized] = Encoders.product[SingleErrorStardardized]
    implicit val dceErrorEncoder: Encoder[DceError] = Encoders.product[DceError]

    val allowedErrorCodes = KafkaErrorSenderPluginImpl.errorCodesForSource(params.sourceId)

    val reportDateCol: Column = if (dataFrame.columns.contains(ColumnNames.reportDate)) {
      col(ColumnNames.reportDate)
    } else {
      typedLit[Option[java.sql.Date]](None).as(ColumnNames.reportDate)
    }

    val stdErrors = dataFrame
      // only keep rows with non-empty errCol:
      .filter(size(col("errCol")) > 0)
      // and only keep columns that are needed for the actual error publishing:
      .select(
        col(ColumnNames.enceladusRecordId).cast(DataTypes.StringType).as("recordId"),
        reportDateCol,
        explode(col(ColumnNames.errCol)).as("singleError")
      )
      .as[SingleErrorStardardized]
      .map(_.toDceError(params))
      .filter(entry => allowedErrorCodes.contains(entry.errorCode)) // Std xor Conf error codes
      .toDF()

    stdErrors
  }

  def prepareDataForKafka(stdErrors: DataFrame, params: ErrorSenderPluginParams, keyConfig: ToAvroConfig,
                          valueConfig: ToAvroConfig): DataFrame = {
    val valueSchemaType = KafkaErrorSenderPlugin.getValueStructTypeSchema

    val allValueColumns = struct(stdErrors.columns.head, stdErrors.columns.tail: _*)

    stdErrors.sqlContext.createDataFrame(stdErrors.rdd, valueSchemaType) // forces avsc schema to assure compatible nullability of the DF
      .select(
        to_avro(
          struct(lit(params.sourceSystem).as("sourceSystem")), keyConfig).as("key"),
        to_avro(allValueColumns, valueConfig).as("value")
      )
  }

  /**
   * Actual data sending
   * @param df Dataframe with confluent_avro columns - key & value
   */
  private[mq] def sendErrorsToKafka(df: DataFrame): Unit = {
    require(df.schema.fieldNames.contains("key") && df.schema.fieldNames.contains("value"))

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", connectionParams.bootstrapServers)
      .option("topic", connectionParams.topicName)
      .option("kafka.client.id", connectionParams.clientId)
      .withOptionalKafkaSecurityParams(connectionParams.security)
      .option("path", "notReallyUsedButAtumExpectsItToBePresent") // TODO Atum issue #32
      .save()
  }
}

object KafkaErrorSenderPluginImpl {

  // columns from the original dataframe (post Standardization/Conformance) to be addressed
  object ColumnNames {
    val enceladusRecordId = "enceladus_record_id"
    val reportDate = "reportDate"
    val errCol = "errCol"
  }

  case class ErrorRecord(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

  case class SingleErrorStardardized(recordId: String, reportDate: java.sql.Date, singleError: ErrorRecord) {
    def toDceError(additionalParams: ErrorSenderPluginParams): DceError = {
      import DceError.{additionalInfoKeys => key}

      DceError(
        sourceSystem = additionalParams.sourceSystem,
        sourceSystemId = None,
        dataset = Some(additionalParams.datasetName),
        ingestionNumber = None,
        processingTimestamp = additionalParams.processingTimestamp.toEpochMilli,
        informationDate = Option(reportDate).map(_.toLocalDate.toEpochDay.toInt),
        outputFileName = Some(additionalParams.outputPath),
        recordId = recordId,
        errorSourceId = additionalParams.sourceId.value,
        errorType = singleError.errType,
        errorCode = singleError.errCode,
        errorDescription = singleError.errMsg,
        additionalInfo = Map(
          key.reportDate -> additionalParams.reportDate,
          key.reportVersion -> additionalParams.reportVersion.toString,
          key.datasetName -> additionalParams.datasetName,
          key.datasetVersion -> additionalParams.datasetVersion.toString
        ) ++ additionalParams.uniqueRunId.fold(Map.empty[String, String])(runId => Map(key.uniqueRunId -> runId))
          ++ additionalParams.runId.fold(Map.empty[String, String])(runId => Map(key.runId -> runId.toString))
          ++ additionalParams.runUrls.fold(Map.empty[String, String])(runUrls => Map(key.runUrl -> runUrls))
      )
    }
  }

  def errorCodesForSource(sourceId: SourcePhase): Seq[String] = sourceId match {
    case SourcePhase.Standardization => ErrorCodes.standardizationErrorCodes
    case SourcePhase.Conformance => ErrorCodes.conformanceErrorCodes
  }

  implicit class DataFrameWriterOptionExt[T](dataFrameWriter: DataFrameWriter[T]) {
    def withOptionalKafkaSecurityParams(optSecParams: Option[KafkaSecurityParams]): DataFrameWriter[T] = {
      optSecParams match {
        case None => dataFrameWriter
        case Some(secParams) => dataFrameWriter.options(secParams.toMap)
      }
    }
  }

}
