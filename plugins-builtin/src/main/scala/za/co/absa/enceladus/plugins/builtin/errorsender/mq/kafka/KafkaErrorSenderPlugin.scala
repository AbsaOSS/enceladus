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

package za.co.absa.enceladus.plugins.builtin.errorsender.mq.kafka

import com.typesafe.config.Config
import org.apache.avro.{Schema => AvroSchema}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorFactory
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.KafkaErrorSenderPluginImpl

/**
 * Implementation of a plugin factory that creates the above plugin based on configuration passed from
 * Enceladus Spark application.
 */
object KafkaErrorSenderPlugin extends PostProcessorFactory {
  val ClientIdKey = "kafka.error.client.id"
  val ErrorKafkaTopicKey = "kafka.error.topic.name"

  object Key {
    val avroSchemaResource = "/dq_errors_key_avro_schema.avsc"

    // values these must match the name/namespace in the avsc above
    val recordName = "dataErrorKey"
    val namespaceName = "za.co.absa.dataquality.errors.avro.key.schema"
  }

  object Value {
    val avroSchemaResource = "/dq_errors_avro_schema.avsc"

    // values these must match the name/namespace in the avsc above
    val recordName = "dataError"
    val namespaceName = "za.co.absa.dataquality.errors.avro.schema"
  }

  override def apply(config: Config): KafkaErrorSenderPluginImpl = {
    val connectionParams = kafkaConnectionParamsFromConfig(config)
    val valueSchemaRegistryConfig = avroValueSchemaRegistryConfig(connectionParams)
    val keySchemaRegistryConfig = avroKeySchemaRegistryConfig(connectionParams)

    KafkaErrorSenderPluginImpl(connectionParams, keySchemaRegistryConfig, valueSchemaRegistryConfig)
  }

  def kafkaConnectionParamsFromConfig(config: Config): KafkaConnectionParams = {
    KafkaConnectionParams.fromConfig(config, ClientIdKey, ErrorKafkaTopicKey)
  }

  def avroValueSchemaRegistryConfig(connectionParams: KafkaConnectionParams): ToAvroConfig = {
    // generate schema
//    val allColumns = struct(df.columns.map(c => df(c)): _*)
//    val expression = allColumns.expr
//    val schema = toAvroType(expression.dataType, expression.nullable)

    val schemaRegistryClientConfig =
      Map(AbrisConfig.SCHEMA_REGISTRY_URL -> connectionParams.schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val subject = SchemaSubject.usingTopicRecordNameStrategy(connectionParams.topicName, Value.recordName, Value.namespaceName)
    val schemaId = schemaManager.register(subject, "schemaString")//schould pass schema here

    AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(connectionParams.schemaRegistryUrl)
  }

  def avroKeySchemaRegistryConfig(connectionParams: KafkaConnectionParams): ToAvroConfig = {
//     generate schema
//    val allColumns = struct(dataFrame.columns.map(c => dataFrame(c)): _*)
//    val expression = allColumns.expr
//    val schema = toAvroType(expression.dataType, expression.nullable)

    val schemaRegistryClientConfig =
      Map(AbrisConfig.SCHEMA_REGISTRY_URL -> connectionParams.schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val subject = SchemaSubject.usingTopicRecordNameStrategy(connectionParams.topicName, Key.recordName, Key.namespaceName)
    val schemaId = schemaManager.register(subject, "schema")//should pass schema here)

    AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(connectionParams.schemaRegistryUrl)
  }

  private def getAvroSchemaString(resourcePath: String): String = {
    val schemaStream = getClass.getResourceAsStream(resourcePath)
    val schemaString = IOUtils.toString(schemaStream, "UTF-8")
    schemaStream.close()

    schemaString
  }

  def getKeyAvroSchemaString: String = getAvroSchemaString(Key.avroSchemaResource)

  def getValueAvroSchemaString: String = getAvroSchemaString(Value.avroSchemaResource)

  private def getStructTypeSchema(avroSchemaString: String): StructType = {
    val schema = new AvroSchema.Parser().parse(avroSchemaString)
    SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }

  def getValueStructTypeSchema: StructType = getStructTypeSchema(getValueAvroSchemaString)

  def getKeyStructTypeSchema: StructType = getStructTypeSchema(getKeyAvroSchemaString)
}
