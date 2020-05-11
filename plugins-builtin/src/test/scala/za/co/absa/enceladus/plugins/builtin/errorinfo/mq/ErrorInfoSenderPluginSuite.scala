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

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams.ErrorSourceId
import za.co.absa.enceladus.plugins.builtin.errorinfo.DceErrorInfo
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPluginSuite.{TestingErrCol, TestingRecord}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ErrorInfoSenderPluginSuite extends FlatSpec with SparkTestBase with Matchers {

  final val StdCastError = "E00000"
  final val ConfMapError = "E00001"
  final val StdNullError = "E00002"
  final val ConfCastErr = "E00003"
  final val ConfNegErr = "E00004"
  final val ConfLitErr = "E00005"
  final val StdTypeError = "E00006"
  final val StdSchemaError = "E00007"

  val testData = Seq(
    TestingRecord("enceladusId1", java.sql.Date.valueOf("2020-02-20"), Seq(
      TestingErrCol("confMapError", "E00001", "Conformance Mapping Error", "bogusTopic", Seq("someValue1")),
      TestingErrCol("confCastError", "E00003", "Conformance Cast Error - Null returned by casting conformance rule", "bogusTopic", Seq("someValue3")),
      TestingErrCol("confNegErr", "E00004", "Conformance Negation Error", "bogusTopic", Seq("someValue4")),
      TestingErrCol("confLitErr", "E00005", "Conformance Literal Error", "bogusTopic", Seq("someValue5"))
    )),
    TestingRecord("enceladusId2", java.sql.Date.valueOf("2020-02-21"), Seq(
      TestingErrCol("stdCastError", "E00000", "Standardization Error - Type cast", "sub.field1", Seq("someValue")),
      TestingErrCol("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "field2,", Seq("someValue2")),
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
    val plugin = new ErrorInfoSenderPlugin(null, Map(), Map())

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = ErrorSourceId.Standardization))
      .as[DceErrorInfo].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("stdCastError", "E00000"),
      ("stdNullError", "E00002"),
      ("stdTypeError", "E00006"),
      ("stdSchemaError", "E00007")
    )
  }

  it should "getIndividualErrors (exploding, filtering by source for Conformance)" in {
    val plugin = new ErrorInfoSenderPlugin(null, Map(), Map())

    plugin.getIndividualErrors(testDataDf, defaultPluginParams.copy(sourceId = ErrorSourceId.Conformance))
      .as[DceErrorInfo].collect.map(entry => (entry.errorType, entry.errorCode)) should contain theSameElementsAs Seq(
      ("confMapError", "E00001"),
      ("confCastError", "E00003"),
      ("confNegErr", "E00004"),
      ("confLitErr", "E00005")
    )
  }
}

object ErrorInfoSenderPluginSuite {

  case class TestingRecord(enceladus_record_id: String, reportDate: java.sql.Date, errCol: Seq[TestingErrCol])

  case class TestingErrCol(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String])

}
