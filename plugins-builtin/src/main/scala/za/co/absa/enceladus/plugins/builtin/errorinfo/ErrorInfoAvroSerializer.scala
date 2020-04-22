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

package za.co.absa.enceladus.plugins.builtin.errorinfo

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.plugins.builtin.common.mq.InfoAvroSerializer

import scala.util.control.NonFatal

object ErrorInfoAvroSerializer extends InfoAvroSerializer[DceErrorInfo] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Converts a error info key represented by a name of a dataset to an Avro record. // todo redo, what should be the key?
   *
   * @param errorInfo A name of a dataset.
   * @return An Avro record representing a key to error info topic.
   */
  override def convertInfoKey(errorInfo: DceErrorInfo): GenericRecord = {
    // todo what should be the key?; key now shared with info file
    val keyAvroSchemaJson = IOUtils.toString(getClass.getResourceAsStream("/info_file_key_avro_schema.avsc"), "UTF-8")
    val keyAvroSchema = new Schema.Parser().parse(keyAvroSchemaJson)
    val avroKey = new GenericData.Record(keyAvroSchema)

    avroKey.put("datasetName", errorInfo.sourceDataset)

    avroKey
  }

  /**
   * Converts a error info record to an Avro record.
   *
   * @param errorInfo An instance of error info record.
   * @return An Avro record representing a value to error info topic.
   */
  override def convertInfoRecord(errorInfo: DceErrorInfo): Option[GenericRecord] = {
    val recordAvroSchema = getAvroSchema
    val avroErrorInfo = new GenericData.Record(recordAvroSchema)

    import AvroFieldNames._

    try {
      avroErrorInfo.put(sourceSystem, errorInfo.sourceSystem)
      avroErrorInfo.put(sourceDataset, errorInfo.sourceDataset)
      avroErrorInfo.put(processingTimestamp, errorInfo.processingTimestamp)
      avroErrorInfo.put(informationDate, errorInfo.informationDate)
      avroErrorInfo.put(outputFileName, errorInfo.outputFileName)
      avroErrorInfo.put(recordId, errorInfo.recordId)
      avroErrorInfo.put(errorSourceId, errorInfo.errorSourceId.toString)
      avroErrorInfo.put(errorType, errorInfo.errorType)
      avroErrorInfo.put(errorCode, errorInfo.errorCode)
      avroErrorInfo.put(errorColumn, errorInfo.errorColumn)
      avroErrorInfo.put(errorValue, errorInfo.errorValue)
      avroErrorInfo.put(errorDescription, errorInfo.errorDescription)
      avroErrorInfo.put(additionalDetails, errorInfo.additionalDetails)

      Option(avroErrorInfo)
    } catch {
      case NonFatal(ex) =>
        logger.error("Error converting error info to Avro.", ex)
        None
    }
  }

  private def getAvroSchema: Schema = {
    val infoFileSchema = IOUtils.toString(getClass.getResourceAsStream("/error_info_file_avro_schema.avsc"), "UTF-8")
    new Schema.Parser().parse(infoFileSchema)
  }


  object AvroFieldNames {
    val sourceSystem = "sourceSystem"
    val sourceDataset = "sourceDataset"
    val processingTimestamp = "processingTimestamp"
    val informationDate = "informationDate"
    val outputFileName = "outputFileName"
    val recordId = "recordId"
    val errorSourceId = "errorSourceId"
    val errorType = "errorType"
    val errorCode = "errorCode"
    val errorColumn = "errorColumn"
    val errorValue = "errorValue"
    val errorDescription = "errorDescription"
    val additionalDetails = "additionalDetails"
  }

}
