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

package za.co.absa.enceladus.plugins.builtin.common.mq.kafka

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.plugins.builtin.common.mq.ControlInfoProducer
import za.co.absa.enceladus.plugins.builtin.controlinfo.{ControlInfoAvroSerializer, DceControlInfo}

import scala.util.control.NonFatal


/**
 * This class is responsible for sending control measurements to Kafka.
 */
class ControlInfoProducerKafka(kafkaConnectionParams: KafkaConnectionParams) extends ControlInfoProducer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val kafkaProducer: KafkaProducer[GenericRecord, GenericRecord] = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionParams.bootstrapServers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConnectionParams.schemaRegistryUrl)
    props.put(CommonClientConfigs.CLIENT_ID_CONFIG, kafkaConnectionParams.clientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    kafkaConnectionParams.security.foreach(sec => {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sec.securityProtocol)
      sec.saslMechanism.foreach(saslMechanism => props.put("sasl.mechanism", saslMechanism))
    })

    new KafkaProducer[GenericRecord, GenericRecord](props)
  }

  /**
   * Sends control info measurements to a Kafka topic.
   *
   * @param controlInfo An instance of Atum control measurements plus information identifying the dataset and
   *                    the state of the job.
   */
  def send(controlInfo: DceControlInfo): Unit = {
    val avroKey = ControlInfoAvroSerializer.convertControlInfoKey(controlInfo.datasetName)
    val avroRecordOpt = ControlInfoAvroSerializer.convertControlInfoRecord(controlInfo)

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

  /**
   * This method is called when the Kafka producer is no longer needed.
   */
  override def close(): Unit = {
    kafkaProducer.flush()
    kafkaProducer.close()
  }
}
