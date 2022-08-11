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

package za.co.absa.enceladus.standardization.config

import java.time.ZonedDateTime
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.dao.auth.{RestApiKerberosCredentials, RestApiPlainCredentials}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.StandardizationExecution
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase

class StandardizationParserSuite extends AnyFunSuite with TZNormalizedSparkTestBase {

  private val year = "2018"
  private val month = "12"
  private val day = "31"
  private val dateTokens = Array(year, month, day)
  private val hdfsRawPath = "/bigdatahdfs/datalake/raw/system/feed"
  private val hdfsRawPathOverride = "/bigdatahdfs/datalake/raw/system/feed/override"
  private val hdfsPublishPath = "/bigdatahdfs/datalake/publish/system/feed"
  private val restApiCredentialsFile = "src/test/resources/rest-api-credentials.conf"
  private val restApiCredentials = RestApiPlainCredentials.fromFile(restApiCredentialsFile)
  private val keytabPath = "src/test/resources/user.keytab.example"
  private val restApiKeytab = RestApiKerberosCredentials("user@EXAMPLE.COM", keytabPath)
  private val datasetName = "test-dataset-name"
  private val datasetVersion = 2
  private val description = None
  private val schemaName = "test-schema-name"
  private val schemaVersion = 4
  private val dateCreated = ZonedDateTime.now()
  private val userCreated = "user"
  private val lastUpdated = ZonedDateTime.now()
  private val userUpdated = "user"
  private val reportDate = s"$year-$month-$day"
  private val disabled = false
  private val dateDisabled = None
  private val userDisabled = None
  private val locked = None
  private val dateLocked = None
  private val userLocked = None
  private val reportVersion = 3
  private val rawFormat = "parquet"
  private val folderPrefix = s"year=$year/month=$month/day=$day"

  implicit val hadoopConf = spark.sparkContext.hadoopConfiguration

  private object TestStandardization extends StandardizationExecution

  test("Test credentials file parsing "){
    val credentials = RestApiPlainCredentials.fromFile(restApiCredentialsFile)

    assert(credentials.username == "user")
    assert(credentials.password == "changeme")
  }

  test("Test keytab file parsing "){
    val credentials = RestApiKerberosCredentials.fromFile(keytabPath)

    assert(credentials === restApiKeytab)
  }

  test("folder-prefix parameter") {
    val cmdConfigNoFolderPrefix = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--raw-format", rawFormat))

    val actualPlainRestApiCredentials = cmdConfigNoFolderPrefix.restApiCredentialsFactory.getInstance()

    assert(cmdConfigNoFolderPrefix.datasetName === datasetName)
    assert(cmdConfigNoFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigNoFolderPrefix.reportDate === reportDate)
    assert(cmdConfigNoFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigNoFolderPrefix.rawFormat === rawFormat)
    assert(cmdConfigNoFolderPrefix.folderPrefix.isEmpty)
    assert(cmdConfigNoFolderPrefix.rawPathOverride.isEmpty)
    assert(actualPlainRestApiCredentials === restApiCredentials)

    val cmdConfigFolderPrefix = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-auth-keytab", keytabPath,
        "--raw-format", rawFormat,
        "--folder-prefix", folderPrefix))

    val actualRestApiKerberosCredentials = cmdConfigFolderPrefix.restApiCredentialsFactory.getInstance()

    assert(cmdConfigFolderPrefix.datasetName === datasetName)
    assert(cmdConfigFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigFolderPrefix.reportDate === reportDate)
    assert(cmdConfigFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigFolderPrefix.rawFormat === rawFormat)
    assert(cmdConfigFolderPrefix.folderPrefix.nonEmpty)
    assert(cmdConfigFolderPrefix.folderPrefix.get === folderPrefix)
    assert(cmdConfigFolderPrefix.rawPathOverride.isEmpty)
    assert(actualRestApiKerberosCredentials === restApiKeytab)
  }

  test("Test buildRawPath") {
    val standardiseDataset = Dataset(
      datasetName,
      datasetVersion,
      description,
      hdfsRawPath,
      hdfsPublishPath,
      schemaName,
      schemaVersion,
      dateCreated,
      userCreated,
      lastUpdated,
      userUpdated,
      disabled,
      dateDisabled,
      userDisabled,
      locked,
      dateLocked,
      userLocked,
      List()
    )
    val cmdConfigNoFolderPrefix = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--raw-format", rawFormat))
    val cmdConfigFolderPrefix = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--folder-prefix", folderPrefix,
        "--raw-format", rawFormat))
    val cmdConfigRawPathOverride = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--debug-set-raw-path", hdfsRawPathOverride,
        "--raw-format", rawFormat))
    val cmdConfigRawPathOverrideAndFolderPrefix = StandardizationConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--folder-prefix", folderPrefix,
        "--debug-set-raw-path", hdfsRawPathOverride,
        "--raw-format", rawFormat))

    val publishPathNoFolderPrefix = TestStandardization.getPathConfig(cmdConfigNoFolderPrefix, standardiseDataset,
        cmdConfigRawPathOverride.reportVersion.get).raw.path
    assert(publishPathNoFolderPrefix === s"${standardiseDataset.hdfsPath}/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmdConfigNoFolderPrefix.reportVersion.get}")
    val publishPathFolderPrefix = TestStandardization
      .getPathConfig(cmdConfigFolderPrefix, standardiseDataset, cmdConfigRawPathOverride.reportVersion.get).raw.path
    assert(publishPathFolderPrefix === s"${standardiseDataset.hdfsPath}/$folderPrefix/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmdConfigFolderPrefix.reportVersion.get}")
    val publishPathRawPathOverride = TestStandardization
      .getPathConfig(cmdConfigRawPathOverride, standardiseDataset, cmdConfigRawPathOverride.reportVersion.get).raw.path
    assert(publishPathRawPathOverride === hdfsRawPathOverride)
    val publishPathRawPathOverrideAndFolderPrefix = TestStandardization
      .getPathConfig(cmdConfigRawPathOverrideAndFolderPrefix, standardiseDataset,
        cmdConfigRawPathOverride.reportVersion.get).raw.path
    assert(publishPathRawPathOverrideAndFolderPrefix === hdfsRawPathOverride)
  }

}
