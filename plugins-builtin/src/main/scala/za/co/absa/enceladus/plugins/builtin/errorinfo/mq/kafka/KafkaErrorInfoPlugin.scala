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

package za.co.absa.enceladus.plugins.builtin.errorinfo.mq.kafka

import com.typesafe.config.Config
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorFactory
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin

/**
 * Implementation of a plugin factory that creates the above plugin based on configuration passed from
 * Enceladus Spark application.
 */
object KafkaErrorInfoPlugin extends PostProcessorFactory {
  val ClientIdKey = "kafka.errorinfo.client.id"
  val ErrorInfoKafkaTopicKey = "kafka.errorinfo.topic.name"

  override def apply(config: Config): ErrorInfoSenderPlugin = {
    val connectionParams = KafkaConnectionParams.fromConfig(config, ClientIdKey, ErrorInfoKafkaTopicKey)

    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> connectionParams.schemaRegistryUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> connectionParams.topicName,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "error.info.strategy1", // todo what should this be set to?
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "co.za.absa.dataquality.avro.schema-errorinfo"
    )

    new ErrorInfoSenderPlugin(connectionParams, schemaRegistryConfig)
  }
}
