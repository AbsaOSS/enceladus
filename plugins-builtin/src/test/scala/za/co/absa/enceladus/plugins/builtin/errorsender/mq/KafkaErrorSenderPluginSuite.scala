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

package za.co.absa.enceladus.plugins.builtin.errorsender.mq

import java.time.Instant

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.{KafkaConnectionParams, KafkaSecurityParams, SchemaRegistrySecurityParams}
import za.co.absa.enceladus.plugins.builtin.errorsender.DceError
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.KafkaErrorSenderPluginSuite.{TestingErrCol, TestingRecord}
import za.co.absa.enceladus.plugins.builtin.errorsender.mq.kafka.KafkaErrorSenderPlugin
import za.co.absa.enceladus.plugins.builtin.errorsender.params.ErrorSenderPluginParams
import za.co.absa.enceladus.utils.modules.SourcePhase
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase


class KafkaErrorSenderPluginSuite extends AnyFlatSpec with TZNormalizedSparkTestBase with Matchers with BeforeAndAfterAll {

  private val port = 6081
  private val wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port))

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  private val testData = Seq(
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

  private val testDataDf = testData.toDF
  private val testNow = Instant.now()

  private val defaultPluginParams = ErrorSenderPluginParams(
    "datasetName1", datasetVersion = 1, "2020-03-30", reportVersion = 1, "output/Path1", null,
    "sourceSystem1", Some("http://runUrls1"), runId = Some(1), Some("uniqueRunId"), testNow)

  "ErrorSenderPluginParams" should "getIndividualErrors (exploding, filtering by source for Standardization)" in {
    val plugin = KafkaErrorSenderPluginImpl(null)

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = SourcePhase.Standardization))
      .as[DceError].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("stdCastError", "E00000"),
      ("stdNullError", "E00002"),
      ("stdTypeError", "E00006"),
      ("stdSchemaError", "E00007")
    )
  }

  it should "getIndividualErrors (exploding, filtering by source for Conformance)" in {
    val plugin = KafkaErrorSenderPluginImpl(null)

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = SourcePhase.Conformance))
      .as[DceError].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("confMapError", "E00001"),
      ("confCastError", "E00003"),
      ("confNegErr", "E00004"),
      ("confLitErr", "E00005")
    )
  }

  val testClientId = "errorId1"
  val testTopicName = "errorTopicName1"
  val testKafkaUrl = "http://example.com:9092"
  val testSecurityProtocol = "SASL_SSL"
  val testSaslMechanism = "GSSAPI"
  val testSchemaRegUrl = "mock://example.com:8081"
  val testSchemaRegAuthSource = "USER_INFO"
  val testSchemaRegAuthUserInfo = "svc-account:SVC-P4SSW0RD"

  private val testConfig = ConfigFactory.empty()
    .withValue("kafka.error.client.id", ConfigValueFactory.fromAnyRef(testClientId))
    .withValue("kafka.error.topic.name", ConfigValueFactory.fromAnyRef(testTopicName))
    .withValue("kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(testKafkaUrl))
    .withValue("kafka.security.protocol", ConfigValueFactory.fromAnyRef(testSecurityProtocol))
    .withValue("kafka.sasl.mechanism", ConfigValueFactory.fromAnyRef(testSaslMechanism))
    .withValue("kafka.schema.registry.url", ConfigValueFactory.fromAnyRef(testSchemaRegUrl))
    .withValue("kafka.schema.registry.basic.auth.credentials.source", ConfigValueFactory.fromAnyRef(testSchemaRegAuthSource))
    .withValue("kafka.schema.registry.basic.auth.user.info", ConfigValueFactory.fromAnyRef(testSchemaRegAuthUserInfo))

  it should "correctly create the error plugin from config" in {
    val errorPlugin: KafkaErrorSenderPluginImpl = KafkaErrorSenderPlugin.apply(testConfig)

    errorPlugin.connectionParams shouldBe KafkaConnectionParams(bootstrapServers = testKafkaUrl,
      schemaRegistryUrl = testSchemaRegUrl, clientId = testClientId,
      security = Some(KafkaSecurityParams(testSecurityProtocol, Some(testSaslMechanism))), topicName = testTopicName,
      schemaRegistrySecurityParams = Some(SchemaRegistrySecurityParams(testSchemaRegAuthSource, Some(testSchemaRegAuthUserInfo))))
  }

  it should "correctly register schemas" in {
    val connectionParams = KafkaErrorSenderPlugin.kafkaConnectionParamsFromConfig(testConfig)

    KafkaErrorSenderPlugin.registerSchemas(connectionParams) match {
      case (_, _, schemaConfig) => // we do not care about `keySchemaId`, `valueSchemaId` assigned
        schemaConfig shouldBe Map(
          "schema.registry.url" -> "mock://example.com:8081",
          "basic.auth.credentials.source" -> "USER_INFO",
          "basic.auth.user.info" -> "svc-account:SVC-P4SSW0RD"
        )
    }
  }

  it should "skip sending 0 errors to kafka" in {
    val connectionParams = KafkaErrorSenderPlugin.kafkaConnectionParamsFromConfig(testConfig)

    var sendErrorsToKafkaWasCalled = false
    val errorKafkaPlugin = new KafkaErrorSenderPluginImpl(connectionParams) {
      override private[mq] def sendErrorsToKafka(df: DataFrame): Unit = {
        sendErrorsToKafkaWasCalled = true
        fail("Sending should have been skipped for 0 errors")
      }
    }

    // onlyConformanceErrorsDataDf should result in 0 std errors
    val onlyConformanceErrorsDataDf =  Seq(testData(1)).toDF
    errorKafkaPlugin.onDataReady(onlyConformanceErrorsDataDf, defaultPluginParams.copy(sourceId = SourcePhase.Standardization).toMap)

    assert(!sendErrorsToKafkaWasCalled, "KafkaErrorSenderPluginImpl.sentErrorToKafka should not be called for 0 errors")
  }

  it should "fail on incompatible parameters map" in {
    val errorPlugin: KafkaErrorSenderPluginImpl = KafkaErrorSenderPlugin.apply(testConfig)
    val bogusParamMap = Map("bogus" -> "boo")

    val caughtException = the[IllegalArgumentException] thrownBy {
      errorPlugin.onDataReady(testDataDf, bogusParamMap)
    }
    caughtException.getMessage should include ("Incompatible parameter map supplied")
    caughtException.getCause shouldBe a[NoSuchElementException]
  }

  Seq(
    SourcePhase.Standardization -> Seq(
      "Standardization,stdCastError,E00000,Standardization Error - Type cast",
      "Standardization,stdNullError,E00002,Standardization Error - Null detected in non-nullable attribute"
    ),
    SourcePhase.Conformance -> Seq(
      "Conformance,confNegErr,E00004,Conformance Negation Error",
      "Conformance,confLitErr,E00005,Conformance Literal Error"
    )
  ).foreach { case (source, specificErrorParts) =>
    it should s"send $source errors to kafka as confluent_avro" in {

      val configWithMockedRegistry = ConfigFactory.empty()
        .withValue("kafka.error.client.id", ConfigValueFactory.fromAnyRef("errorId1"))
        .withValue("kafka.error.topic.name", ConfigValueFactory.fromAnyRef("errorTopicId1"))
        .withValue("kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef("http://bogus-kafka:9092"))
        .withValue("kafka.schema.registry.url", ConfigValueFactory.fromAnyRef(s"http://localhost:$port"))

      object Expected {
        val keySchema = """"schema":"{\"type\":\"record\",\"name\":\"dataErrorKey\",\"namespace\":\"za.co.absa.dataquality.errors.avro.key.schema\",\"fields\":[{\"name\":\"sourceSystem\",\"type\":\"string\"}]}""""
        val valueSchema = """"schema" :"{\"type\":\"record\",\"name\":\"dataError\",\"namespace\":\"za.co.absa.dataquality.errors.avro.schema\",\"fields\":[{\"name\":\"sourceSystem\",\"type\":\"string\"},{\"name\":\"sourceSystemId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dataset\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ingestionNumber\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"processingTimestamp\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},{\"name\":\"informationDate\",\"type\":[\"null\",\"int\"],\"default\":null,\"logicalType\":\"date\"},{\"name\":\"outputFileName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"recordId\",\"type\":\"string\"},{\"name\":\"errorSourceId\",\"type\":\"string\"},{\"name\":\"errorType\",\"type\":\"string\"},{\"name\":\"errorCode\",\"type\":\"string\"},{\"name\":\"errorDescription\",\"type\":\"string\"},{\"name\":\"additionalInfo\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}""""

      }

      object Aux {
        val keyId = 1
        val valueId = 2
        val notFoundBody = """{"error_code":40401,"message":"Subject not found."}"""
      }

      Seq(
        "key" -> (Expected.keySchema, Aux.keyId),
        "value" -> (Expected.valueSchema, Aux.valueId)
      ).foreach { case (item, (schema, id)) =>
        // first,  the key/value is not known
        wireMockServer.stubFor(get(urlPathMatching(s"/subjects/errorTopicId1-$item/versions/latest"))
          .willReturn(notFound().withBody(Aux.notFoundBody)))

        // allows to register the schema in the schema registry, return assigned id
        wireMockServer.stubFor(
          post(urlPathEqualTo(s"/subjects/errorTopicId1-$item/versions"))
            .withRequestBody(equalToJson("{"+schema+"}"))
            .willReturn(okJson(s"""{"id":$id}""")))

        wireMockServer.stubFor(get(urlPathMatching(s"/subjects/errorTopicName1-$item/versions/latest"))
          .willReturn(okJson( s"""{"id":${id},"version":1,${schema}}""")))

        wireMockServer.stubFor(get(urlPathMatching(s"/schemas/ids/$id"))
          .willReturn(okJson(s"""{"id":${id},"version":1,${schema}}""")))
      }

      val smallDf = testDataDf.limit(1).toDF()

      val connectionParams = KafkaErrorSenderPlugin.kafkaConnectionParamsFromConfig(configWithMockedRegistry)

      val errorKafkaPlugin = new KafkaErrorSenderPluginImpl(connectionParams) {
        override private[mq] def sendErrorsToKafka(df: DataFrame): Unit = {
          import org.apache.spark.sql.functions.col
          import za.co.absa.abris.avro.functions.from_avro

          // at the point of usage from_avro, key/value.schema.id must be part of the SR Config:
          val keyConfigWithId = AbrisConfig
            .fromConfluentAvro
            .downloadReaderSchemaByLatestVersion
            .andTopicNameStrategy(testTopicName, isKey = true)
            .usingSchemaRegistry(connectionParams.schemaRegistryUrl)

          val valueConfigWithId = AbrisConfig
            .fromConfluentAvro
            .downloadReaderSchemaByLatestVersion
            .andTopicNameStrategy(testTopicName)
            .usingSchemaRegistry(connectionParams.schemaRegistryUrl)

          val dataKeyStrings = df.select(from_avro(col("key"), keyConfigWithId)).collect().toSeq.map(_.toString())
          val dataValueStrings = df.select(from_avro(col("value"), valueConfigWithId)).collect().toSeq.map(_.toString())

          val expectedKeyStrings = Seq("[[sourceSystem1]]", "[[sourceSystem1]]")
          val expectedValueStrings = specificErrorParts.map { specificPart =>
            s"""[[sourceSystem1,null,datasetName1,null,${testNow.toEpochMilli},18311,output/Path1,enceladusId1,$specificPart,Map(runUrl -> http://runUrls1, datasetVersion -> 1, uniqueRunId -> uniqueRunId, datasetName -> datasetName1, reportVersion -> 1, reportDate -> 2020-03-30, runId -> 1)]]"""
          }

          dataKeyStrings should contain theSameElementsAs expectedKeyStrings
          dataValueStrings should contain theSameElementsAs expectedValueStrings
        }
      }

      // commence the confluent_avro processing
      errorKafkaPlugin.onDataReady(smallDf, defaultPluginParams.copy(sourceId = source).toMap)

      // verifying, that all expected schema registry url has been called
      // this is, however, imperfect, because we are not checking count and wiremock is being not reset (problematic)
      Seq(
        "key" -> (Expected.keySchema, Aux.keyId),
        "value" -> (Expected.valueSchema, Aux.valueId)
      ).foreach { case (item, (schema, id)) =>
        wireMockServer.verify(postRequestedFor(urlPathEqualTo(s"/subjects/errorTopicId1-$item/versions")).withRequestBody(equalToJson("{"+schema+"}")))
        wireMockServer.verify(getRequestedFor(urlPathEqualTo(s"/subjects/errorTopicName1-$item/versions/latest")))
      }

    }

  }

}

object KafkaErrorSenderPluginSuite {

  case class TestingRecord(enceladus_record_id: String, reportDate: java.sql.Date, errCol: Seq[TestingErrCol])

  case class TestingErrCol(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

}
