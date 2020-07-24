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

package za.co.absa.enceladus.plugins.builtin.errorsender.params

import java.time.Instant

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.utils.modules.SourcePhase

class ErrorSenderPluginParamsSuite extends FlatSpec with Matchers {

  private val params = ErrorSenderPluginParams(
    datasetName = "datasetName1",
    datasetVersion = 1,
    reportDate = "2020-03-30",
    reportVersion = 1,
    outputPath = "output/Path1",
    sourceId = SourcePhase.Conformance,
    sourceSystem = "sourceSystem1",
    runUrls = Some("http://runUrls1"),
    runId = Some(1),
    uniqueRunId = Some("uniqueRunId"),
    processingTimestamp = Instant.parse("2020-05-12T10:15:30.00Z")
  )

  val paramsMap = Map(
    "datasetName" -> "datasetName1",
    "datasetVersion" -> "1",
    "reportDate" -> "2020-03-30",
    "reportVersion" -> "1",
    "outputPath" -> "output/Path1",
    "sourceId" -> "conformance",
    "sourceSystem" -> "sourceSystem1",
    "runUrls" -> "http://runUrls1",
    "runId" -> "1",
    "uniqueRunId" -> "uniqueRunId",
    "processingTimestamp" -> "2020-05-12T10:15:30Z"
  )

  "ErrorSenderPluginParams" should "serialize to Map[String, String]" in {
    params.toMap shouldBe paramsMap
  }

  it should "deserialize from Map[String, String]" in {
    params shouldBe ErrorSenderPluginParams.fromMap(paramsMap)
  }

}
