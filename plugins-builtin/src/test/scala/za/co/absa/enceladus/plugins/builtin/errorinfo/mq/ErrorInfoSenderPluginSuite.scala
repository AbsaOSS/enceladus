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

package za.co.absa.enceladus.plugins.builtin.errorinfo.mq

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams.ErrorSourceId
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPluginSuite.{TestingErrCol, TestingRecord}
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.kafka.KafkaErrorInfoPlugin
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ErrorInfoSenderPluginSuite extends FlatSpec with SparkTestBase with Matchers {

  val testData = Seq(
    TestingRecord("enceladusId1", java.sql.Date.valueOf("2020-02-20"), Seq(
      TestingErrCol("stdCastError", "E00000", "Standardization Error - Type cast", "sub.field1", Seq("someValue")),
      TestingErrCol("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "field2,", Seq("someValue2"))
    )),
    TestingRecord("enceladusId1", java.sql.Date.valueOf("2020-02-20"), Seq(
      TestingErrCol("confNegErr", "E00004", "Conformance Negation Error", "bogusTopic", Seq("someValue4")),
      TestingErrCol("confLitErr", "E00005", "Conformance Literal Error", "bogusTopic", Seq("someValue5"))
    )),
    TestingRecord("enceladusId2", java.sql.Date.valueOf("2020-02-21"), Seq(
      TestingErrCol("confMapError", "E00001", "Conformance Mapping Error", "bogusTopic", Seq("someValue1")),
      TestingErrCol("confCastError", "E00003", "Conformance Cast Error - Null returned by casting conformance rule", "bogusTopic", Seq("someValue3"))
    )),
    TestingRecord("enceladusId2", java.sql.Date.valueOf("2020-02-21"), Seq(
      TestingErrCol("stdTypeError", "E00006", "Standardization Type Error", "field2,", Seq("someValue2")),
      TestingErrCol("stdSchemaError", "E00007", "Standardization Schema Error", "field2,", Seq("someValue2"))
    ))
  )

  import spark.implicits._

  val testDataDf = testData.toDF

  val defaultPluginParams = PostProcessorPluginParams(
    "datasetName1", datasetVersion = 1, "2020-03-30", reportVersion = 1, "output/Path1", null,
    "sourceSystem1", Some("http://runUrls1"), runId = Some(1), Some("uniqueRunId"))

  "ErrorInfoSenderPluginSuite" should "getIndividualErrors (exploding, filtering by source for Standardization)" in {
    val plugin = ErrorInfoSenderPlugin(null, Map(), Map())

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = ErrorSourceId.Standardization))
      .as[DceErrorInfo].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("stdCastError", "E00000"),
      ("stdNullError", "E00002"),
      ("stdTypeError", "E00006"),
      ("stdSchemaError", "E00007")
    )
  }

  it should "getIndividualErrors (exploding, filtering by source for Conformance)" in {
    val plugin = ErrorInfoSenderPlugin(null, Map(), Map())

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = ErrorSourceId.Conformance))
      .as[DceErrorInfo].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("confMapError", "E00001"),
      ("confCastError", "E00003"),
      ("confNegErr", "E00004"),
      ("confLitErr", "E00005")
    )
  }

  it should "correctly create the error plugin from config" in {
    val testClientId = "errorId1"
    val testTopicName = "errorTopicName1"
    val testKafkaUrl = "http://example.com:9092"
    val testSchemaRegUrl = "http://example.com:8081"

    val config = ConfigFactory.empty()
      .withValue("kafka.errorinfo.client.id", ConfigValueFactory.fromAnyRef(testClientId))
      .withValue("kafka.errorinfo.topic.name", ConfigValueFactory.fromAnyRef(testTopicName))
      .withValue("kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(testKafkaUrl))
      .withValue("kafka.schema.registry.url", ConfigValueFactory.fromAnyRef(testSchemaRegUrl))

    val errorPlugin: ErrorInfoSenderPlugin = KafkaErrorInfoPlugin.apply(config)

    errorPlugin.connectionParams shouldBe KafkaConnectionParams(bootstrapServers = testKafkaUrl, schemaRegistryUrl = testSchemaRegUrl,
      clientId = testClientId, security = None, topicName = testTopicName)

    errorPlugin.keySchemaRegistryConfig shouldBe Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> testSchemaRegUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> testTopicName,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "dataErrorKey",
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "za.co.absa.dataquality.errors.avro.key.schema")

    errorPlugin.valueSchemaRegistryConfig shouldBe Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> testSchemaRegUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> testTopicName,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "dataError",
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "za.co.absa.dataquality.errors.avro.schema")
  }
}

object ErrorInfoSenderPluginSuite {

  case class TestingRecord(enceladus_record_id: String, reportDate: java.sql.Date, errCol: Seq[TestingErrCol])

  case class TestingErrCol(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

}
