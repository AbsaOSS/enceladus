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

package za.co.absa.enceladus.conformance

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

object HyperConformanceAttributes {

  // Configuration keys expected to be set up when running Conformance as a Transformer component for Hyperdrive
  val restApiUriKey = "enceladus.rest.uri"
  val restApiUriRetryCountKey = "enceladus.rest.retryCount"
  val restApiAvailabilitySetupKey = "enceladus.rest.availability.setup"
  val restApiCredentialsFileKey = "enceladus.rest.credentials.file"
  val restApiAuthKeytabKey = "enceladus.rest.auth.keytab"
  val datasetNameKey = "dataset.name"
  val datasetVersionKey = "dataset.version"
  val reportDateKey = "report.date"
  val reportVersionKey = "report.version"
  val reportVersionColumnKey = "report.version.column"
  val eventTimestampColumnKey = "event.timestamp.column"
  val eventTimestampPatternKey = "event.timestamp.pattern"
}

trait HyperConformanceAttributes extends HasComponentAttributes {
  import HyperConformanceAttributes._

  override def getName: String = "HyperConformance"

  override def getDescription: String = "Dynamic Conformance - a transformer component for Hyperdrive"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    restApiUriKey ->
      PropertyMetadata("REST API URL", Some("E.g. http://localhost:8080/rest_api"), required = true),
    restApiUriRetryCountKey ->
      PropertyMetadata("REST API URL retry count",
        Some("How many times a call to REST API URL should be retried after failure before proceeding to the next URL. E.g. 2"),
        required = false),
    restApiAvailabilitySetupKey ->
      PropertyMetadata("The setup type of REST API URLs",
        Some("""Either "roundrobin" (default) or "fallback", affects in which order the URls are picked up for use. """ +
          "Round-robin - start from random, fallback - start from first"),
        required = false),
    datasetNameKey ->
      PropertyMetadata("Dataset name", None, required = true),
    datasetVersionKey ->
      PropertyMetadata("Dataset version", None, required = true),
    reportDateKey ->
      PropertyMetadata("Report date", Some("The current date is used by default"), required = false),
    reportVersionColumnKey ->
      PropertyMetadata("Report version column", Some("Taken from another column"), required = false),
    reportVersionKey ->
      PropertyMetadata("Report version", Some("Will be determined automatically by default if not specified"), required = false),
    eventTimestampColumnKey ->
      PropertyMetadata("Event timestamp column",
        Some("If specified, the column will be used to generate 'enceladus_info_date'"),
        required = false),
    eventTimestampPatternKey ->
      PropertyMetadata("Event timestamp pattern",
        Some("If the event timestamp column is string, the pattern is used to parse it (default is 'yyyy-MM-dd'T'HH:mm'Z''"),
        required = false),
    restApiCredentialsFileKey ->
      PropertyMetadata("REST API credentials file", Some("Relative or absolute path to the file on local FS or HDFS"), required = false),
    restApiAuthKeytabKey ->
      PropertyMetadata("Keytab", Some("Relative or absolute path to the file on local FS or HDFS"), required = false)
  )

  override def getExtraConfigurationPrefix: Option[String] = None
}
