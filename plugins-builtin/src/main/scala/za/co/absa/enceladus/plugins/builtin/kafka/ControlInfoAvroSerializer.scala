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

package za.co.absa.enceladus.plugins.builtin.kafka

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import za.co.absa.atum.model.{Checkpoint, ControlMeasureMetadata}

import scala.util.control.NonFatal

object ControlInfoAvroSerializer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Converts a control info key represented by a name of a dataset to an Avro record.
   *
   * @param datasetName A name of a dataset.
   * @return An Avro record representing a key to control info topic.
   */
  def convertControlInfoKey(datasetName: String): GenericRecord = {
    val keyAvroSchemaJson = IOUtils.toString(getClass.getResourceAsStream("/info_file_key_avro_schema.avsc"), "UTF-8")
    val keyAvroSchema = new Schema.Parser().parse(keyAvroSchemaJson)
    val avroKey = new GenericData.Record(keyAvroSchema)

    avroKey.put("datasetName", datasetName)

    avroKey
  }

  /**
   * Converts a control info measurements to an Avro record.
   *
   * @param controlInfo An instance of control measurements.
   * @return An Avro record representing a key to control info topic.
   */
  def convertControlInfoRecord(controlInfo: DceControlInfo): Option[GenericRecord] = {
    val recordAvroSchema = getAvroSchema
    val avroDceControlInfo = new GenericData.Record(recordAvroSchema)

    try {
      val avroMetadata = convertMetadata(controlInfo.controlMeasure.metadata, recordAvroSchema)
      val avroCheckpoints = convertCheckpoints(controlInfo.controlMeasure.checkpoints, recordAvroSchema)

      avroDceControlInfo.put("datasetName", controlInfo.datasetName)
      avroDceControlInfo.put("datasetVersion", controlInfo.datasetVersion)
      avroDceControlInfo.put("reportDate", controlInfo.reportDate)
      avroDceControlInfo.put("reportVersion", controlInfo.reportVersion)
      avroDceControlInfo.put("runStatus", controlInfo.runStatus)
      avroDceControlInfo.put("metadata", avroMetadata)
      avroDceControlInfo.put("checkpoints", avroCheckpoints)

      Option(avroDceControlInfo)
    } catch {
      case NonFatal(ex) =>
        logger.error("Error converting control measurements to Avro.", ex)
        None
    }
  }

  private def getAvroSchema: Schema = {
    val infoFileSchema = IOUtils.toString(getClass.getResourceAsStream("/info_file_avro_schema.avsc"), "UTF-8")
    new Schema.Parser().parse(infoFileSchema)
  }

  private def convertMetadata(metadata: ControlMeasureMetadata, recordSchema: Schema): GenericRecord = {
    val avroMetadata = new GenericData.Record(recordSchema.getField("metadata").schema)

    val arrayAddInfoSchema = avroMetadata.getSchema.getField("additionalInfo").schema
    val addInfoSize = metadata.additionalInfo.size
    val itemSchemaAddInfo = arrayAddInfoSchema.getElementType

    val arrayAddInfo = new GenericData.Array[GenericRecord](addInfoSize, arrayAddInfoSchema)
    metadata.additionalInfo.foreach { case (key, value) =>
      val nestedAddInfo: GenericRecord = new GenericData.Record(itemSchemaAddInfo)
      nestedAddInfo.put("key", key)
      nestedAddInfo.put("value", value)
      arrayAddInfo.add(nestedAddInfo)
    }

    avroMetadata.put("sourceApplication", metadata.sourceApplication)
    avroMetadata.put("country", metadata.country)
    avroMetadata.put("historyType", metadata.historyType)
    avroMetadata.put("dataFilename", metadata.dataFilename)
    avroMetadata.put("sourceType", metadata.sourceType)
    avroMetadata.put("version", metadata.version)
    avroMetadata.put("informationDate", metadata.informationDate)
    avroMetadata.put("additionalInfo", arrayAddInfo)

    avroMetadata
  }

  private def convertCheckpoints(chckpoints: List[Checkpoint], recordSchema: Schema): GenericData.Array[GenericRecord] = {
    val checkpointsArraySchema = recordSchema.getField("checkpoints").schema

    val checkpointSchema = checkpointsArraySchema.getElementType
    val measurementsArraySchema = checkpointSchema.getField("controls").schema

    val avroCheckpoints = new GenericData.Array[GenericRecord](chckpoints.size, checkpointsArraySchema)
    val measurementSchema = measurementsArraySchema.getElementType

    chckpoints.foreach(checkpoint => {
      val avroCheckpoint = new GenericData.Record(checkpointSchema)
      val avroMeasurements = new GenericData.Array[GenericRecord](checkpoint.controls.size, measurementsArraySchema)

      avroCheckpoint.put("name", checkpoint.name)
      avroCheckpoint.put("processStartTime", checkpoint.processStartTime)
      avroCheckpoint.put("processEndTime", checkpoint.processEndTime)
      avroCheckpoint.put("workflowName", checkpoint.workflowName)
      avroCheckpoint.put("order", checkpoint.order)
      avroCheckpoint.put("controls", avroMeasurements)
      avroCheckpoints.add(avroCheckpoint)

      checkpoint.controls.foreach(measurement => {
        val avroMeasurement = new GenericData.Record(measurementSchema)
        avroMeasurement.put("controlName", measurement.controlName)
        avroMeasurement.put("controlType", measurement.controlType)
        avroMeasurement.put("controlCol", measurement.controlCol)
        avroMeasurement.put("controlValue", measurement.controlValue)
        avroMeasurements.add(avroMeasurement)
      })
    })
    avroCheckpoints
  }

}
