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

package za.co.absa.enceladus.common.config

import org.apache.spark.storage.StorageLevel
import scopt.OParser
import za.co.absa.enceladus.dao.auth.{InvalidRestApiCredentialsFactory, RestApiCredentialsFactory, RestApiKerberosCredentialsFactory, RestApiPlainCredentialsFactory}
import za.co.absa.enceladus.utils.general.ProjectMetadata

import scala.util.matching.Regex


trait JobConfigParser[R] {
  def withDatasetName(value: String): R
  def withDatasetVersion(value: Int): R
  def withReportDate(value: String): R
  def withReportVersion(value: Option[Int]): R
  def withPerformanceMetricsFile(value: Option[String]): R
  def withFolderPrefix(value: Option[String]): R
  def withCredsFile(value: Option[String], restApiCredentialsFactory: RestApiCredentialsFactory): R
  def withAuthKeytab(value: Option[String], restApiCredentialsFactory: RestApiCredentialsFactory): R
  def withPersistStorageLevel(value: Option[StorageLevel]): R

  def datasetName: String
  def reportDate: String
  def restApiCredentialsFactory: RestApiCredentialsFactory
  def datasetVersion: Int
  def reportVersion: Option[Int]
  def performanceMetricsFile: Option[String]
  def folderPrefix: Option[String]
  def persistStorageLevel: Option[StorageLevel]
  def credsFile: Option[String]
  def keytabFile: Option[String]
}

object JobConfigParser extends ProjectMetadata {

  //scalastyle:off method.length the length is legit for parsing input paramters
  def jobConfigParser[R <: JobConfigParser[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(

      head(s"Enceladus v${projectVersion}"),
      opt[String]('D', "dataset-name").required().action((value, config) =>
        config.withDatasetName(value)).text("Dataset name"),

      opt[Int]('d', "dataset-version").required().action((value, config) =>
        config.withDatasetVersion(value)).text("Dataset version")
        .validate(value =>
          if (value > 0) {
            success
          } else {
            failure("Option --dataset-version must be > 0")
          }),

      opt[String]('R', "report-date").required().action((value, config) =>
        config.withReportDate(value)).text("Report date in 'yyyy-MM-dd' format")
        .validate(value => {
          val reportDateMatcher: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r
          reportDateMatcher.findFirstIn(value) match {
            case None => failure(s"Match error in '$value'. Option --report-date expects a date in 'yyyy-MM-dd' format")
            case _ => success
          }
        }),

      opt[Int]('r', "report-version").optional().action((value, config) =>
        config.withReportVersion(Some(value)))
        .text("Report version. If not provided, it is inferred based on the publish path (it's an EXPERIMENTAL feature)")
        .validate(value =>
          if (value > 0) {
            success
          } else {
            failure("Option --report-version must be >0")
          }),

      opt[String]("rest-api-credentials-file").hidden.optional().action({ (file, config) =>
        config.withCredsFile(Option(file), new RestApiPlainCredentialsFactory(file))
      }).text("Path to REST API credentials config file."),

      opt[String]("rest-api-auth-keytab").optional().action({ (file, config) => {
          config.withAuthKeytab(Option(file), new RestApiKerberosCredentialsFactory(file))
      }
      }).text("Path to keytab file used for authenticating to REST API."),


      opt[String]("performance-file").optional().action((value, config) =>
        config.withPerformanceMetricsFile(Option(value)))
        .text("Produce a performance metrics file at the given location (local filesystem)"),

      opt[String]("folder-prefix").optional().action((value, config) =>
        config.withFolderPrefix(Option(value))).text("Adds a folder prefix before the infoDateColumn"),

      opt[String]("persist-storage-level").optional().action((value, config) =>
        config.withPersistStorageLevel(Some(StorageLevel.fromString(value))))
        .text("Specifies persistence storage level to use when processing data. Spark's default is MEMORY_AND_DISK."),

      checkConfig { config =>
        config.restApiCredentialsFactory match {
          case InvalidRestApiCredentialsFactory => failure("No authentication method specified (e.g. --rest-api-auth-keytab)")
          case _ if config.credsFile.isDefined && config.keytabFile.isDefined =>
            failure("Only one authentication method is allowed at a time")
          case _ => success
        }
      }
    )
  }
  //scalastyle:on method.length
}
