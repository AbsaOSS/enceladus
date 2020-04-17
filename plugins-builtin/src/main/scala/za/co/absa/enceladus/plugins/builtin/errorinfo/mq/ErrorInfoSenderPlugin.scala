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
import org.apache.spark.sql.DataFrame
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessor
import za.co.absa.enceladus.plugins.builtin.common.mq.InfoProducer
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo

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
    val stdCount = dataFrame.count()

    import org.apache.spark.sql.functions.{col, size}
    val stdErrors = dataFrame.filter(size(col("errCol")) > 0)
    val errCount = stdErrors.count()
    log.info(s"*** STD count = $stdCount, errCount = $errCount") // debug

    producer.send(DceErrorInfo(
      sourceSystem = "testSystem",
      sourceDataset = "testDataSet",
      informationDate = "2022-02-22",
      processingDate = LocalDate.now().toString,
      recordId = s"errorCnt=$errCount",
      errorCode = "E345"
    ))

    dataFrame
  }
}
