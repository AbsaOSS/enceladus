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
import org.apache.avro.{Schema => AvroSchema}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
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

  val valueAvroSchemaResource = "/dq_errors_avro_schema.avsc"

  // these must match the name/namespace in the avsc above
  val recordName = "dataError"
  val namespaceName = "za.co.absa.dataquality.errors.avro.schema"

  override def apply(config: Config): ErrorInfoSenderPlugin = {
    val connectionParams = KafkaConnectionParams.fromConfig(config, ClientIdKey, ErrorInfoKafkaTopicKey)

    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> connectionParams.schemaRegistryUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> connectionParams.topicName,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> recordName,
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> namespaceName
    )

    new ErrorInfoSenderPlugin(connectionParams, schemaRegistryConfig)
  }

  def getAvroSchemaString: String = {
    val schemaStream = getClass.getResourceAsStream(valueAvroSchemaResource)
    val schemaString = IOUtils.toString(schemaStream, "UTF-8")
    schemaStream.close()

    schemaString
  }

  def getStructTypeSchema: StructType = {
    val schema = new AvroSchema.Parser().parse(getAvroSchemaString)
    SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }
}
