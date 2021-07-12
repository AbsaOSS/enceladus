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

package za.co.absa.enceladus.plugins.buildin.kafka

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.plugins.buildin.factories.DceControlInfoFactory
import za.co.absa.enceladus.plugins.buildin.kafka.dummy.DummyControlInfoProducer
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.{KafkaConnectionParams, KafkaSecurityParams, SchemaRegistrySecurityParams}
import za.co.absa.enceladus.plugins.builtin.controlinfo.mq.ControlInfoSenderPlugin

import scala.collection.JavaConverters._

class KafkaPluginSuite extends AnyFunSuite {
  test("Test Kafka info plugin sends control measurements") {
    val producer = new DummyControlInfoProducer
    val dceControlInfo = DceControlInfoFactory.getDummyDceControlInfo()
    val kafkaPlugin = new ControlInfoSenderPlugin(producer)

    kafkaPlugin.onCheckpoint(dceControlInfo.controlMeasure, Map[String, String](
      "datasetName" -> dceControlInfo.datasetName,
      "datasetVersion" -> dceControlInfo.datasetVersion.toString,
      "reportDate" -> dceControlInfo.reportDate,
      "reportVersion" -> dceControlInfo.reportVersion.toString,
      "runStatus" -> dceControlInfo.runStatus
    ))

    assert(producer.lastControlInfoSent == dceControlInfo)
  }

  test("Test Kafka connection parameters are parsed from a config") {
    val conf = ConfigFactory.parseMap(
      Map[String, String](KafkaConnectionParams.BootstrapServersKey -> "127.0.0.1:8081",
        KafkaConnectionParams.SchemaRegistryUrlKey -> "localhost:9092",
        "my.client.id" -> "dummyClientId",
        "my.topic.name" -> "dummyTopicName").asJava)

    val kafkaConnection = KafkaConnectionParams.fromConfig(conf, "my.client.id", "my.topic.name")

    assert(kafkaConnection.security.isEmpty)
    assert(kafkaConnection.bootstrapServers == "127.0.0.1:8081")
    assert(kafkaConnection.schemaRegistryUrl == "localhost:9092")
    assert(kafkaConnection.clientId == "dummyClientId")
    assert(kafkaConnection.topicName == "dummyTopicName")
  }

  {
    val conf = ConfigFactory.parseMap(
      Map[String, String](KafkaConnectionParams.BootstrapServersKey -> "127.0.0.1:8081",
        KafkaConnectionParams.SchemaRegistryUrlKey -> "localhost:9092",
        "my.client.id" -> "dummyClientId",
        "my.topic.name" -> "dummyTopicName",
        KafkaSecurityParams.SecurityProtocolKey -> "SASL_SSL",
        KafkaSecurityParams.SaslMechanismKey -> "GSSAPI",
        SchemaRegistrySecurityParams.BasicAuthCredentialsSourceKey -> "USER_INFO",
        SchemaRegistrySecurityParams.BasicAuthUserInfoKey -> "user1:password2"
      ).asJava)
    def testCreateKafkaConnection = KafkaConnectionParams.fromConfig(conf, "my.client.id", "my.topic.name")

    test("Test Kafka config parser recognizes security parameters") {
      val kafkaConnection = testCreateKafkaConnection

      assert(kafkaConnection.security.isDefined)
      assert(kafkaConnection.security.get.securityProtocol == "SASL_SSL")
      assert(kafkaConnection.security.get.saslMechanism.isDefined)
      assert(kafkaConnection.security.get.saslMechanism.get == "GSSAPI")
      assert(kafkaConnection.schemaRegistrySecurityParams.get.credentialsSource == "USER_INFO")
      assert(kafkaConnection.schemaRegistrySecurityParams.get.userInfo.get == "user1:password2")
      assert(kafkaConnection.bootstrapServers == "127.0.0.1:8081")
      assert(kafkaConnection.schemaRegistryUrl == "localhost:9092")
      assert(kafkaConnection.clientId == "dummyClientId")
      assert(kafkaConnection.topicName == "dummyTopicName")
    }

    test("KafkaSecurityParams correctly converts to a Map (for a DataFrameWriter usage)") {
      val kafkaConnection = testCreateKafkaConnection

      val expected = Some(Map(
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.mechanism" -> "GSSAPI"
      ))

      assert(kafkaConnection.security.map(_.toMap) == expected)
    }
  }

  test("Test Kafka config parser throws an exception if required properties are missing") {
    val conf = ConfigFactory.parseMap(
      Map[String, String](KafkaConnectionParams.BootstrapServersKey -> "127.0.0.1:8081",
        KafkaConnectionParams.SchemaRegistryUrlKey -> "localhost:9092"
      ).asJava)

    intercept[IllegalArgumentException] {
      KafkaConnectionParams.fromConfig(conf, "my.client.id", "my.topic.name")
    }
  }

}
