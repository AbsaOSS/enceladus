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

import com.typesafe.config.Config

case class KafkaConnectionParams(
                          bootstrapServers: String,
                          schemaRegistryUrl: String,
                          clientId: String,
                          topicName: String
                          )

object KafkaConnectionParams {
  val BootstrapServersKey = "kafka.bootstrap.servers"
  val SchemaRegistryUrlKey = "kafka.schema.registry.url"
  val ClientIdKey = "kafka.client.id"
  val TopicNameKey = "kafka.topic.name"

  def fromConfig(conf: Config): KafkaConnectionParams = {
    validate(conf)

    KafkaConnectionParams(conf.getString(BootstrapServersKey),
      conf.getString(SchemaRegistryUrlKey),
      conf.getString(ClientIdKey),
      conf.getString(TopicNameKey)
    )
  }

  private def validate(config: Config): Unit = {

  }
}
