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
                                  security: Option[KafkaSecurityParams],
                                  topicName: String
                                )

object KafkaConnectionParams {
  val BootstrapServersKey = "kafka.bootstrap.servers"
  val SchemaRegistryUrlKey = "kafka.schema.registry.url"

  /**
   * Creates an instance of connection parameters base on the provided configuration.
   * The client Id and the topic name keys are required to be provided explicitly.
   * This is since we can have other plugins that use Kafka and they
   * can reuse Kafka connection endpoints while having their own
   * client id and topic name.
   *
   * @param conf         A configuration.
   * @param clientIdKey  A configuration key that specifies a client Id.
   * @param topicNameKey A configuration key that specifies a topic name.
   * @return An instance of Kafka connection parameters.
   */
  @throws[IllegalArgumentException]
  def fromConfig(conf: Config, clientIdKey: String, topicNameKey: String): KafkaConnectionParams = {
    validate(conf, clientIdKey, topicNameKey)

    KafkaConnectionParams(conf.getString(BootstrapServersKey),
      conf.getString(SchemaRegistryUrlKey),
      conf.getString(clientIdKey),
      KafkaSecurityParams.fromConfig(conf),
      conf.getString(topicNameKey)
    )
  }

  @throws[IllegalArgumentException]
  private def validate(conf: Config, clientIdKey: String, topicNameKey: String): Unit = {
    val requiredFields = BootstrapServersKey :: SchemaRegistryUrlKey :: clientIdKey :: topicNameKey :: Nil

    val missingKeys = requiredFields.filterNot(conf.hasPath)

    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Missing Kafka configuration keys: ${missingKeys.mkString(", ")}.")
    }
  }
}
