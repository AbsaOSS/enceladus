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

import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.plugins.builtin.common.mq.{InfoAvroSerializer, InfoProducer}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


/**
 * This class is responsible for sending info records to Kafka.
 */
class InfoProducerKafka[T](kafkaConnectionParams: KafkaConnectionParams)
                          (implicit serializer: InfoAvroSerializer[T]) extends InfoProducer[T] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val kafkaProducer: KafkaProducer[GenericRecord, GenericRecord] = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionParams.bootstrapServers)
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConnectionParams.schemaRegistryUrl)
    props.put(CommonClientConfigs.CLIENT_ID_CONFIG, kafkaConnectionParams.clientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    kafkaConnectionParams.security.foreach(sec => {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sec.securityProtocol)
      sec.saslMechanism.foreach(saslMechanism => props.put("sasl.mechanism", saslMechanism))
    })

    kafkaConnectionParams.schemaRegistrySecurityParams.foreach(param => {
      props.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, param.credentialsSource)
      param.userInfo.foreach(info => props.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, info))
    })

    new KafkaProducer[GenericRecord, GenericRecord](props)
  }

  /**
   * Sends info records to a Kafka topic.
   *
   * @param infoRecord a value class serializable with acorresponding InfoAvroSerializer to Avro to be sent to kafka.
   */
  def send(infoRecord: T): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val avroKey = serializer.convertInfoKey(infoRecord)
    val avroRecordOpt = serializer.convertInfoRecord(infoRecord)

    try {
      avroRecordOpt match {
        case Some(avroRecord) =>
          logger.info(s"Sending info records ${infoRecord.getClass.getName} to ${kafkaConnectionParams.topicName}...")
          val producerRecord = new ProducerRecord[GenericRecord, GenericRecord](kafkaConnectionParams.topicName,
            avroKey, avroRecord)

          // A future should be started first, since conversion from Java future to Scala future is asynchronous.
          // We need to make sure the sending process is started so that if close() is called, it will wait for
          // the send to finish.
          val sendFuture = kafkaProducer.send(producerRecord)

          // Java Future to Scala Future
          // Note that the conversion doesn't happen immediately since the body is specified by name.
          // That's why we need to actually invoke '.send()' earlier and pass already existing future here.
          Future[RecordMetadata] {
            sendFuture.get
          }.onComplete {
            case Success(metadata) =>
              logger.info(s"Info records were sent successfully to the Kafka topic, offset=${metadata.offset()}.")
            case Failure(ex) =>
              logger.error(s"Failed to send control measurements to Kafka.", ex)
          }
        case None =>
          logger.error(s"No Avro records generated from info record $infoRecord.")
      }
    } catch {
      case NonFatal(ex) => logger.error(s"Error sending info record $infoRecord to Kafka.", ex)
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
