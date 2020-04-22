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

import java.time.LocalDate

import org.apache.log4j.LogManager
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders}
import za.co.absa.enceladus.plugins.api.postprocessor.{PostProcessor, PostProcessorPluginParams}
import za.co.absa.enceladus.plugins.builtin.common.mq.InfoProducer
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin.SingleErrorStardardized
import org.apache.spark.sql.functions.{col, explode, size}

class ErrorInfoSenderPlugin(connectionParams: KafkaConnectionParams) extends PostProcessor {

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

    val stdCount = dataFrame.count()

    implicit val singleErrorStardardizedEncoder = Encoders.product[SingleErrorStardardized]
    implicit val dceErrorInfoEncoder = Encoders.product[DceErrorInfo]

    val stdErrors = dataFrame
      .filter(size(col("errCol")) > 0)
      .select(col("tradeId"), col("reportDate"), explode(col("errCol")).as("singleError"))
      .as[SingleErrorStardardized]
      .map(_.toErrorInfo(params))
      .toDF()

    val errCount = stdErrors.count()
    log.info(s"*** STD count = $stdCount, errCount = $errCount") // debug
    stdErrors
  }

  def sendErrorsToKafka(stdErrors: DataFrame, params: Map[String, String]): Unit = {
    // todo change columns that are used here for key and value
    stdErrors.limit(10).select(
      col("recordId").as("key").cast(DataTypes.StringType),
      col("errorDescription").as("value").cast(DataTypes.StringType)
    )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", connectionParams.bootstrapServers)
      .option("topic", connectionParams.topicName)
      .option("kafka.client.id", connectionParams.clientId)
      .option("path", "notReallyUsedButAtumExpectsItToBePresent") // TODO unhook atum in SparkQueryExecutionListener for kafka format?
      .save()

    //    kafka.schema.registry.url:"http://127.0.0.1:8081"
    //    kafka.bootstrap.servers="127.0.0.1:9092"
    //    kafka.info.metrics.client.id="controlInfo"
    //    kafka.info.metrics.topic.name="control.info"
    //
    //    # todo change clientId to a proper value
    //    kafka.errorinfo.client.id="errorId123"
    //    kafka.errorinfo.topic.name="error.info"

    // works
    //    producer.send(DceErrorInfo(
    //      sourceSystem = "testSystem",
    //      sourceDataset = params("datasetName"),
    //      processingTimestamp = LocalDate.now().toString, // todo where to get this from?
    //      informationDate = params("reportDate"),
    //      outputFileName = params("outputPath"),
    //      recordId = "item.tradeId.toString",
    //      errorSourceId = params("sourceId"),
    //      errorType = "singleError.errType",
    //      errorCode = "singleError.errCode",
    //      errorColumn = "singleError.errCol",
    //      errorValue = "singleError.rawValues.toString()", // todo is this ok?
    //      errorDescription = "singleError.errMsg",
    //      additionalDetails = s"no details, but errCount=$errCount"
    //    ))


  }
}

object ErrorInfoSenderPlugin {

  case class ErrorRecord(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

  case class SingleErrorStardardized(tradeId: Double, singleError: ErrorRecord) {
    def toErrorInfo(additionalParams: Map[String, String]): DceErrorInfo = DceErrorInfo(
      sourceSystem = "testSystem",
      sourceDataset = additionalParams("datasetName"),
      processingTimestamp = LocalDate.now().toString, // todo where to get this from?
      informationDate = additionalParams("reportDate"),
      outputFileName = additionalParams("outputPath"),
      recordId = tradeId.toString,
      errorSourceId = additionalParams("sourceId"),
      errorType = singleError.errType,
      errorCode = singleError.errCode,
      errorColumn = singleError.errCol,
      errorValue = singleError.rawValues.toString(), // todo is this ok?
      errorDescription = singleError.errMsg,
      additionalDetails = s"???"
    )
  }

}
