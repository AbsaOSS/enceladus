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

package za.co.absa.enceladus.plugins.builtin.errorinfo

import java.time.LocalDate

case class DceErrorInfo(sourceSystem: String,
                        sourceSystemId: Option[String],
                        dataset: Option[String],
                        ingestionNumber: Option[Long],
                        processingTimestamp: Long,
                        informationDate: Option[Int],
                        outputFileName: Option[String],
                        recordId: String,
                        errorSourceId: String, // should hold string values of ErrorSourceId
                        errorType: String,
                        errorCode: String,
                        errorDescription: String,
                        additionalInfo: Map[String, String])

object DceErrorInfo {

  object additionalInfoKeys {
    val reportDate = "reportDate"
    val reportVersion = "reportVersion"
    val datasetName = "datasetName"
    val datasetVersion = "datasetVersion"
    val uniqueRunId = "uniqueRunId"
    val runId = "runId"
    val runUrl = "runUrl"
  }

}
