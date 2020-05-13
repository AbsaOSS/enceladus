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

trait HyperConformanceAttributes extends HasComponentAttributes {

  val keysPrefix = "transformer.hyperconformance"

  // Configuration keys expected to be set up when running Conformance as a Transformer component for Hyperdrive
  val menasUriKey = s"$keysPrefix.menas.rest.uri"
  val menasCredentialsFileKey = s"$keysPrefix.menas.credentials.file"
  val menasAuthKeytabKey = s"$keysPrefix.menas.auth.keytab"
  val datasetNameKey = s"$keysPrefix.dataset.name"
  val datasetVersionKey = s"$keysPrefix.dataset.version"
  val reportDateKey = s"$keysPrefix.report.date"
  val reportVersionKey = s"$keysPrefix.report.version"
  val eventTimestampColumnKey = s"$keysPrefix.event.timestamp.column"
  val eventTimestampPatternKey = s"$keysPrefix.event.timestamp.pattern"

  override def getName: String = "HyperConformance"

  override def getDescription: String = "Dynamic Conformance - a transformer component for Hyperdrive"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    menasUriKey ->
      PropertyMetadata("Menas API URL", Some("E.g. http://localhost:8080/menas"), required = true),
    datasetNameKey ->
      PropertyMetadata("Dataset name", None, required = true),
    datasetVersionKey ->
      PropertyMetadata("Dataset version", None, required = true),
    reportDateKey ->
      PropertyMetadata("Report date", Some("The current date is used by default0"), required = false),
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
    menasCredentialsFileKey ->
      PropertyMetadata("Menas credentials file", Some("Relative or absolute path to the file on local FS or HDFS"), required = false),
    menasAuthKeytabKey ->
      PropertyMetadata("Keytab", Some("Relative or absolute path to the file on local FS or HDFS"), required = false)
  )

  override def getExtraConfigurationPrefix: Option[String] = None
}
