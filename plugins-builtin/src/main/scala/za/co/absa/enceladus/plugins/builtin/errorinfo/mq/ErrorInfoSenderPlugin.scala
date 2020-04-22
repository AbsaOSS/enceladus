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

import org.apache.log4j.LogManager
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders}
import za.co.absa.enceladus.plugins.api.postprocessor.{PostProcessor, PostProcessorPluginParams}
import za.co.absa.enceladus.plugins.builtin.common.mq.InfoProducer
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin.StardardizedRow

class ErrorInfoSenderPlugin(producer: InfoProducer[DceErrorInfo]) extends PostProcessor {

  private val log = LogManager.getLogger(classOf[ErrorInfoSenderPlugin])

  override def close(): Unit = producer.close()

  /**
   * When data is ready, the error info record(s) are pusblished to kafka.
   *
   * @param dataFrame error data only.
   * @param params    Additional key/value parameters provided by Enceladus.
   * @return A dataframe with post processing applied
   */
  override def onDataReady(dataFrame: DataFrame, params: Map[String, String]): DataFrame = {
    // todo do the actual processing here:
    //     naively extracting errColumn data

    sendErrorsToKafka(dataFrame, params)
    dataFrame
  }

  def sendErrorsToKafka(dataFrame: DataFrame, params: Map[String, String]): Unit = {
    val stdCount = dataFrame.count()

    import org.apache.spark.sql.functions.{col, size}
    val stdErrors = dataFrame.filter(size(col("errCol")) > 0)
    val errCount = stdErrors.count()
    log.info(s"*** STD count = $stdCount, errCount = $errCount") // debug

    //implicit val encoder = Encoders.product[StardardizedRow]
    //val stdRows = dataFrame.as[StardardizedRow]

    stdErrors.limit(10).select(col("tradeId").as("key").cast(DataTypes.StringType), col("reportDate").as("value").cast(DataTypes.StringType))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "error.infoX6plugin")
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


//        stdRows.foreach { item =>
//          item.errCol.foreach { singleError =>
//            val errorInfo = DceErrorInfo(
//              sourceSystem = "testSystem",
//              sourceDataset = params("datasetName"),
//              processingTimestamp = Instant.now().toString, // todo where to get this from?
//              informationDate = params("reportDate"),
//              outputFileName = params("outputPath"),
//              recordId = item.tradeId.toString,
//              errorSourceId =  params("sourceId"),
//              errorType = singleError.errType,
//              errorCode = singleError.errCode,
//              errorColumn = singleError.errCol,
//              errorValue = singleError.rawValues.toString(), // todo is this ok?
//              errorDescription = singleError.errMsg,
//              additionalDetails = s"no details, but errCount=$errCount"
//            )
//
//            producer.send(errorInfo)
//
//          }
//        }
  }
}

object ErrorInfoSenderPlugin {

  case class ErrorRecord(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

  case class StardardizedRow(tradeId: Double, errCol: Seq[ErrorRecord])

}
