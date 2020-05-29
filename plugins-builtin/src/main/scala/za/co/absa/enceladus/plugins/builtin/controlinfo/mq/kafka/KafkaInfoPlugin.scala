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

package za.co.absa.enceladus.plugins.builtin.controlinfo.mq.kafka

import com.typesafe.config.Config
import za.co.absa.enceladus.plugins.api.control.{ControlMetricsPlugin, ControlMetricsPluginFactory}
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.{InfoProducerKafka, KafkaConnectionParams}
import za.co.absa.enceladus.plugins.builtin.controlinfo.ControlInfoAvroSerializer
import za.co.absa.enceladus.plugins.builtin.controlinfo.mq.ControlInfoSenderPlugin

/**
 * Implementation of a plugin factory that creates the above plugin based on configuration passed from
 * Enceladus Spark application.
 */
object KafkaInfoPlugin extends ControlMetricsPluginFactory {
  val ClientIdKey = "kafka.info.metrics.client.id"
  val ControlMetricsKafkaTopicKey = "kafka.info.metrics.topic.name"

  implicit val serializer = ControlInfoAvroSerializer

  override def apply(config: Config): ControlMetricsPlugin = {
    val connectionParams = KafkaConnectionParams.fromConfig(config, ClientIdKey, ControlMetricsKafkaTopicKey)
    val producer = new InfoProducerKafka(connectionParams)
    new ControlInfoSenderPlugin(producer)
  }
}
