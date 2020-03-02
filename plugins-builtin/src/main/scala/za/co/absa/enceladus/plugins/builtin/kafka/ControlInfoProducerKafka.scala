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

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory
import za.co.absa.atum.model.{Checkpoint, ControlMeasureMetadata}

import scala.util.control.NonFatal


/**
 * This class is responsible for sending control measurements to Kafka.
 */
class ControlInfoProducerKafka(kafkaConnectionParams: KafkaConnectionParams) extends ControlInfoProducer {
  private val logger = LoggerFactory.getLogger(this.getClass)


  private val recordAvroSchema = getAvroSchema()
  private val kafkaProducer = getKafkaProducer()

  def send(controlInfo: DceControlInfo): Unit = {
    val avroKey = toAvroKey(controlInfo.datasetName)
    val avroRecordOpt = toAvroRecord(controlInfo)

    try {
      avroRecordOpt match {
        case Some(avroRecord) =>
          logger.info(s"Sending control measurements to ${kafkaConnectionParams.topicName}...")
          val producerRecord = new ProducerRecord[GenericRecord, GenericRecord](kafkaConnectionParams.topicName,
            avroKey, avroRecord)
          kafkaProducer.send(producerRecord)
          logger.info("Control measurements were sent successfully to the Kafka topic.")
        case None =>
          logger.error(s"No Avro records generated from control measurements.")
      }
    } catch {
      case NonFatal(ex) => logger.error("Error sending control info metrics to Kafka.", ex)
    }
  }

  private def getAvroSchema(): Schema = {
    val infoFileSchema = IOUtils.toString(getClass.getResourceAsStream("/info_file_avro_schema.avsc"), "UTF-8")
    new Schema.Parser().parse(infoFileSchema)
  }

  private def getKafkaProducer(): KafkaProducer[GenericRecord, GenericRecord] = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionParams.bootstrapServers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConnectionParams.schemaRegistryUrl)
    props.put(CommonClientConfigs.CLIENT_ID_CONFIG, kafkaConnectionParams.clientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    new KafkaProducer[GenericRecord, GenericRecord](props)
  }

  private def convertMetadata(metadata: ControlMeasureMetadata): GenericRecord = {
    val avroMetadata = new GenericData.Record(recordAvroSchema.getField("metadata").schema)

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

  private def convertCheckpoints(chckpoints: List[Checkpoint]): GenericData.Array[GenericRecord] = {
    val checkpointsArraySchema = recordAvroSchema.getField("checkpoints").schema

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

  private def toAvroRecord(controlInfo: DceControlInfo): Option[GenericRecord] = {
    val avroDceControlInfo = new GenericData.Record(recordAvroSchema)

    try {
      val avroMetadata = convertMetadata(controlInfo.controlMeasure.metadata)
      val avroCheckpoints = convertCheckpoints(controlInfo.controlMeasure.checkpoints)

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

  private def toAvroKey(datasetName: String): GenericRecord = {
    val keyAvroSchemaJson = """{"type": "record", "name": "infoKey", "fields": [{"type": "string", "name": "key"}]}}"""
    val keyAvroSchema = new Schema.Parser().parse(keyAvroSchemaJson)
    val avroKey = new GenericData.Record(keyAvroSchema)

    avroKey.put("key", datasetName)

    avroKey
  }

}
