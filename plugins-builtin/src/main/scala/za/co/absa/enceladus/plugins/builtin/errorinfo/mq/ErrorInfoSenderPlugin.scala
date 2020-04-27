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
import org.apache.spark.sql.functions.{col, explode, size, struct, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessor
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin.SingleErrorStardardized
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.kafka.KafkaErrorInfoPlugin

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
  override def onDataReady(dataFrame: DataFrame, params: Map[String, String]): DataFrame = {
    val dfWithErrors = getIndividualErrors(dataFrame, params)
    sendErrorsToKafka(dfWithErrors, params)
    dfWithErrors
  }

  def getIndividualErrors(dataFrame: DataFrame, params: Map[String, String]): DataFrame = {
    implicit val singleErrorStardardizedEncoder = Encoders.product[SingleErrorStardardized]
    implicit val dceErrorInfoEncoder = Encoders.product[DceErrorInfo]

    val stdErrors = dataFrame
      .filter(size(col("errCol")) > 0)
      // only keep columns that are needed for the actual error publishing // todo which are those?
      .select(col("tradeId"), col("reportDate"), explode(col("errCol")).as("singleError"))
      .as[SingleErrorStardardized]
      .map(_.toErrorInfo(params))
      .toDF()

    stdErrors
  }

  def sendErrorsToKafka(stdErrors: DataFrame, params: Map[String, String]): Unit = {

    log.info(s"Sending errors to kafka topic ${connectionParams.topicName} ...")

    val valueAvroSchemaString = KafkaErrorInfoPlugin.getValueAvroSchemaString
    val valueSchemaType = KafkaErrorInfoPlugin.getValueStructTypeSchema

    val keyAvroSchemaString = KafkaErrorInfoPlugin.getKeyAvroSchemaString

    val allValueColumns = struct(stdErrors.columns.head, stdErrors.columns.tail: _*)
    import za.co.absa.abris.avro.functions.to_confluent_avro

    stdErrors.sqlContext.createDataFrame(stdErrors.rdd, valueSchemaType) // forces avsc schema to assure compatible nullability of the DF
      .limit(10) // todo remove when done
      .select(
        to_confluent_avro(
          struct(lit("sourceSystem1").as("sourceSystem")), keyAvroSchemaString, keySchemaRegistryConfig
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

  case class ErrorRecord(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

  case class SingleErrorStardardized(tradeId: Double, reportDate: java.sql.Date, singleError: ErrorRecord) {
    def toErrorInfo(additionalParams: Map[String, String]): DceErrorInfo = DceErrorInfo(
      sourceSystem = "testSystem", // todo load from ATUM
      sourceSystemId = Some("testSystemId"),
      dataset = additionalParams.get("datasetName"),
      ingestionNumber = Some(1L), // todo from where?
      processingTimestamp = Instant.now.toEpochMilli, // todo where to get this from?
      informationDate = Some(reportDate.toLocalDate.toEpochDay.toInt), // todo test it
      outputFileName = additionalParams.get("outputPath"),
      recordId = tradeId.toString, // todo enceladus_record_id
      errorSourceId = additionalParams("sourceId"),
      errorType = singleError.errType,
      errorCode = singleError.errCode,
      errorDescription = singleError.errMsg,
      additionalInfo = Map(
        "uniqueRunId" -> "runId1", // todo replace all these values with actual values it should contain
        "Run URL" -> "url1",
        "reportDate" -> additionalParams("reportDate"),
        "reportVersion" -> "reportVersion1",
        "runId" -> "runId1",
        "datasetName" -> additionalParams("datasetName"),
        "datasetVersion" -> "DatasetVersion1"
      )
    )
  }

}
