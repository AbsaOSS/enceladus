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

import java.time.Instant

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.BeforeAndAfterAll
import za.co.absa.abris.avro.read.confluent.SchemaManager
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams.ErrorSourceId
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaConnectionParams
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPluginSuite.{TestingErrCol, TestingRecord}
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.kafka.KafkaErrorInfoPlugin
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import org.apache.spark.sql.DataFrame

class ErrorInfoSenderPluginSuite extends FlatSpec with SparkTestBase with Matchers with BeforeAndAfterAll {

  private val port = 6081
  private val wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port))

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
   wireMockServer.stop()
  }

  val testData = Seq(
    TestingRecord("enceladusId1", java.sql.Date.valueOf("2020-02-20"), Seq(
      TestingErrCol("stdCastError", "E00000", "Standardization Error - Type cast", "sub.field1", Seq("someValue")),
      TestingErrCol("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "field2,", Seq("someValue2")),
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
  val testNow = Instant.now()

  val defaultPluginParams = PostProcessorPluginParams(
    "datasetName1", datasetVersion = 1, "2020-03-30", reportVersion = 1, "output/Path1", null,
    "sourceSystem1", Some("http://runUrls1"), runId = Some(1), Some("uniqueRunId"), testNow)

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

  Seq(
    ErrorSourceId.Standardization -> Seq(
      "standardizaton,stdCastError,E00000,Standardization Error - Type cast",
      "standardizaton,stdNullError,E00002,Standardization Error - Null detected in non-nullable attribute"
    ),
    ErrorSourceId.Conformance -> Seq(
      "conformance,confNegErr,E00004,Conformance Negation Error",
      "conformance,confLitErr,E00005,Conformance Literal Error"
    )
  ).foreach { case (source, specificErrorParts) =>
    it should s"send $source errors info to kafka as confluent_avro" in {

      val config = ConfigFactory.empty()
        .withValue("kafka.errorinfo.client.id", ConfigValueFactory.fromAnyRef("errorId1"))
        .withValue("kafka.errorinfo.topic.name", ConfigValueFactory.fromAnyRef("errorTopicId1"))
        .withValue("kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef("http://bogus-kafka:9092"))
        .withValue("kafka.schema.registry.url", ConfigValueFactory.fromAnyRef(s"http://localhost:$port"))

      object expected {
        val keySchema = """{"schema":"{\"type\":\"record\",\"name\":\"dataErrorKey\",\"namespace\":\"za.co.absa.dataquality.errors.avro.key.schema\",\"fields\":[{\"name\":\"sourceSystem\",\"type\":\"string\"}]}"}"""
        val valueSchema = """{"schema" :"{\"type\":\"record\",\"name\":\"dataError\",\"namespace\":\"za.co.absa.dataquality.errors.avro.schema\",\"fields\":[{\"name\":\"sourceSystem\",\"type\":\"string\"},{\"name\":\"sourceSystemId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataset\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ingestionNumber\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"processingTimestamp\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},{\"name\":\"informationDate\",\"type\":[\"null\",\"int\"],\"default\":null,\"logicalType\":\"date\"},{\"name\":\"outputFileName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"recordId\",\"type\":\"string\"},{\"name\":\"errorSourceId\",\"type\":\"string\"},{\"name\":\"errorType\",\"type\":\"string\"},{\"name\":\"errorCode\",\"type\":\"string\"},{\"name\":\"errorDescription\",\"type\":\"string\"},{\"name\":\"additionalInfo\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}"}"""
      }

      object aux {
        val keyId = "1"
        val valueId = "2"
        val notFoundBody = """{"error_code":40401,"message":"Subject not found."}"""
      }

      Seq(
        "key" -> (expected.keySchema, aux.keyId),
        "value" -> (expected.valueSchema, aux.valueId)
      ).foreach { case (item, (schema, id)) =>
        // first,  the key/value is not known
        wireMockServer.stubFor(get(s"/subjects/errorTopicId1-$item/versions/latest")
          .willReturn(notFound().withBody(aux.notFoundBody)))

        // allows to register the schema in the schema registry, return assigned id
        wireMockServer.stubFor(
          post(urlPathEqualTo(s"/subjects/errorTopicId1-$item/versions"))
            .withRequestBody(equalToJson(schema))
            .willReturn(okJson(s"""{"id":$id}""")))

        // later, when from_confluent_avro is used to decode from avro, serve the "saved" schema from the registry by id
        wireMockServer.stubFor(get(urlPathEqualTo(s"/schemas/ids/$id"))
          .willReturn(okJson(schema)))
      }

      val smallDf = testDataDf.limit(1).toDF()
      val connectionParams = KafkaErrorInfoPlugin.kafkaConnectionParamsFromConfig(config)
      val keySchemaRegistryConfig = KafkaErrorInfoPlugin.avroKeySchemaRegistryConfig(connectionParams)
      val valueSchemaRegistryConfig = KafkaErrorInfoPlugin.avroValueSchemaRegistryConfig(connectionParams)

      val errorKafkaPlugin = new ErrorInfoSenderPlugin(connectionParams, keySchemaRegistryConfig, valueSchemaRegistryConfig) {
        override private[mq] def sendErrorsToKafka(df: DataFrame): Unit = {
          import za.co.absa.abris.avro.functions.from_confluent_avro
          import org.apache.spark.sql.functions.col

          // at the point of usage from_confluent_avro, key/value.schema.id must be part of the SR Config:
          val keyConfigWithId = keySchemaRegistryConfig.updated(SchemaManager.PARAM_KEY_SCHEMA_ID, aux.keyId)
          val valueConfigWithId = valueSchemaRegistryConfig.updated(SchemaManager.PARAM_VALUE_SCHEMA_ID, aux.valueId)

          val dataKeyStrings = df.select(from_confluent_avro(col("key"), keyConfigWithId)).collect().toSeq.map(_.toString())
          val dataValueStrings = df.select(from_confluent_avro(col("value"), valueConfigWithId)).collect().toSeq.map(_.toString())

          val expectedKeyStrings = Seq("[[sourceSystem1]]", "[[sourceSystem1]]")
          val expectedValueStrings = specificErrorParts.map { specificPart =>
            s"""[[sourceSystem1,null,datasetName1,null,${testNow.toEpochMilli},18312,output/Path1,enceladusId1,$specificPart,Map(runUrl -> http://runUrls1, datasetVersion -> 1, uniqueRunId -> uniqueRunId, datasetName -> datasetName1, reportVersion -> 1, reportDate -> 2020-03-30, runId -> 1)]]"""
          }

          dataKeyStrings should contain theSameElementsAs expectedKeyStrings
          dataValueStrings should contain theSameElementsAs expectedValueStrings
        }
      }

      // commence the confluent_avro processing
      errorKafkaPlugin.onDataReady(smallDf, defaultPluginParams.copy(sourceId = source))

      // verifying, that all expected schema registry url has been called
      // this is, however, imperfect, because we are not checking count and wiremock is being not reset (problematic)
      Seq(
        "key" -> (expected.keySchema, aux.keyId),
        "value" -> (expected.valueSchema, aux.valueId)
      ).foreach { case (item, (schema, id)) =>
        wireMockServer.verify(getRequestedFor(urlPathEqualTo(s"/subjects/errorTopicId1-$item/versions/latest")))
        wireMockServer.verify(postRequestedFor(urlPathEqualTo(s"/subjects/errorTopicId1-$item/versions")).withRequestBody(equalToJson(schema)))
        wireMockServer.verify(getRequestedFor(urlPathEqualTo(s"/schemas/ids/$id")))

      }

    }

  }

}

object ErrorInfoSenderPluginSuite {

  case class TestingRecord(enceladus_record_id: String, reportDate: java.sql.Date, errCol: Seq[TestingErrCol])

  case class TestingErrCol(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

}
