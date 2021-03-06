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

import za.co.absa.enceladus.utils.modules.SourcePhase

case class ErrorSenderPluginParams(datasetName: String,
                                   datasetVersion: Int,
                                   reportDate: String,
                                   reportVersion: Int,
                                   outputPath: String,
                                   sourceId: SourcePhase,
                                   sourceSystem: String,
                                   runUrls: Option[String],
                                   runId: Option[Int],
                                   uniqueRunId: Option[String],
                                   processingTimestamp: Instant
                                ) {

  def toMap: Map[String, String] = ErrorSenderPluginParams.toMap(this)
}

object ErrorSenderPluginParams {

  object FieldNames {
    val datasetName = "datasetName"
    val datasetVersion = "datasetVersion"
    val reportDate = "reportDate"
    val reportVersion = "reportVersion"
    val outputPath = "outputPath"
    val sourceId = "sourceId"
    val sourceSystem = "sourceSystem"
    val runUrls = "runUrls"
    val runId = "runId"
    val uniqueRunId = "uniqueRunId"
    val processingTimestamp = "processingTimestamp"
  }

  import FieldNames._

  def toMap(params: ErrorSenderPluginParams): Map[String, String] = {
    Map(
      datasetName -> params.datasetName,
      datasetVersion -> params.datasetVersion.toString,
      reportDate -> params.reportDate,
      reportVersion -> params.reportVersion.toString,
      outputPath -> params.outputPath,
      sourceId -> params.sourceId.asIdentifier,
      sourceSystem -> params.sourceSystem,
      processingTimestamp -> params.processingTimestamp.toString
    ) ++
      params.uniqueRunId.fold(Map.empty[String, String])(id => Map(uniqueRunId -> id)) ++
      params.runId.fold(Map.empty[String, String])(id => Map(runId -> id.toString)) ++
      params.runUrls.fold(Map.empty[String, String])(urls => Map(runUrls -> urls))
  }

  def fromMap(params: Map[String, String]): ErrorSenderPluginParams = ErrorSenderPluginParams(
    datasetName = params(datasetName),
    datasetVersion = params(datasetVersion).toInt,
    reportDate = params(reportDate),
    reportVersion = params(reportVersion).toInt,
    outputPath = params(outputPath),
    sourceId = SourcePhase.withIdentifier(params(sourceId)),
    sourceSystem = params(sourceSystem),
    runUrls = params.get(runUrls),
    runId = params.get(runId).map(_.toInt),
    uniqueRunId = params.get(uniqueRunId),
    processingTimestamp = Instant.parse(params(processingTimestamp))
  )

}
