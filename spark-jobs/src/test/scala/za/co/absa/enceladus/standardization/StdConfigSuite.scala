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

package za.co.absa.enceladus.standardization

import java.time.ZonedDateTime

import org.scalatest.FunSuite
import za.co.absa.enceladus.dao.auth.{MenasKerberosCredentials, MenasPlainCredentials}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StdConfigSuite extends FunSuite with SparkTestBase {

  private val year = "2018"
  private val month = "12"
  private val day = "31"
  private val dateTokens = Array(year, month, day)
  private val hdfsRawPath = "/bigdatahdfs/datalake/raw/system/feed"
  private val hdfsRawPathOverride = "/bigdatahdfs/datalake/raw/system/feed/override"
  private val hdfsPublishPath = "/bigdatahdfs/datalake/publish/system/feed"
  private val menasCredentialsFile = "src/test/resources/menas-credentials.conf"
  private val menasCredentials = MenasPlainCredentials.fromFile(menasCredentialsFile)
  private val keytabPath = "src/test/resources/user.keytab.example"
  private val menasKeytab = MenasKerberosCredentials("user@EXAMPLE.COM", keytabPath)
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
  private val reportVersion = 3
  private val rawFormat = "parquet"
  private val folderPrefix = s"year=$year/month=$month/day=$day"

  test("Test credentials file parsing "){
    val credentials = MenasPlainCredentials.fromFile(menasCredentialsFile)

    assert(credentials.username == "user")
    assert(credentials.password == "changeme")
  }

  test("Test keytab file parsing "){
    val credentials = MenasKerberosCredentials.fromFile(keytabPath)

    assert(credentials === menasKeytab)
  }

  test("folder-prefix parameter") {
    val cmdConfigNoFolderPrefix = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--raw-format", rawFormat))

    val actualPlainMenasCredentials = cmdConfigNoFolderPrefix.menasCredentialsFactory.getInstance()

    assert(cmdConfigNoFolderPrefix.datasetName === datasetName)
    assert(cmdConfigNoFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigNoFolderPrefix.reportDate === reportDate)
    assert(cmdConfigNoFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigNoFolderPrefix.rawFormat === rawFormat)
    assert(cmdConfigNoFolderPrefix.folderPrefix.isEmpty)
    assert(cmdConfigNoFolderPrefix.rawPathOverride.isEmpty)
    assert(actualPlainMenasCredentials === menasCredentials)

    val cmdConfigFolderPrefix = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-auth-keytab", keytabPath,
        "--raw-format", rawFormat,
        "--folder-prefix", folderPrefix))

    val actualMenasKerberosCredentials = cmdConfigFolderPrefix.menasCredentialsFactory.getInstance()

    assert(cmdConfigFolderPrefix.datasetName === datasetName)
    assert(cmdConfigFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigFolderPrefix.reportDate === reportDate)
    assert(cmdConfigFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigFolderPrefix.rawFormat === rawFormat)
    assert(cmdConfigFolderPrefix.folderPrefix.nonEmpty)
    assert(cmdConfigFolderPrefix.folderPrefix.get === folderPrefix)
    assert(cmdConfigFolderPrefix.rawPathOverride.isEmpty)
    assert(actualMenasKerberosCredentials === menasKeytab)
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
      List()
    )
    val cmdConfigNoFolderPrefix = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--raw-format", rawFormat))
    val cmdConfigFolderPrefix = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--folder-prefix", folderPrefix,
        "--raw-format", rawFormat))
    val cmdConfigRawPathOverride = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--debug-set-raw-path", hdfsRawPathOverride,
        "--raw-format", rawFormat))
    val cmdConfigRawPathOverrideAndFolderPrefix = StdCmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--folder-prefix", folderPrefix,
        "--debug-set-raw-path", hdfsRawPathOverride,
        "--raw-format", rawFormat))


    val publishPathNoFolderPrefix = StandardizationJob.buildRawPath(cmdConfigNoFolderPrefix, standardiseDataset,
        dateTokens, cmdConfigNoFolderPrefix.reportVersion.get)
    assert(publishPathNoFolderPrefix === s"${standardiseDataset.hdfsPath}/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmdConfigNoFolderPrefix.reportVersion.get}")
    val publishPathFolderPrefix = StandardizationJob.buildRawPath(cmdConfigFolderPrefix, standardiseDataset,
        dateTokens, cmdConfigFolderPrefix.reportVersion.get)
    assert(publishPathFolderPrefix === s"${standardiseDataset.hdfsPath}/$folderPrefix/${dateTokens(0)}/${dateTokens(1)}/${dateTokens(2)}/v${cmdConfigFolderPrefix.reportVersion.get}")
    val publishPathRawPathOverride = StandardizationJob.buildRawPath(cmdConfigRawPathOverride, standardiseDataset,
        dateTokens, cmdConfigRawPathOverride.reportVersion.get)
    assert(publishPathRawPathOverride === hdfsRawPathOverride)
    val publishPathRawPathOverrideAndFolderPrefix = StandardizationJob.buildRawPath(cmdConfigRawPathOverrideAndFolderPrefix,
        standardiseDataset, dateTokens, cmdConfigRawPathOverrideAndFolderPrefix.reportVersion.get)
    assert(publishPathRawPathOverrideAndFolderPrefix === hdfsRawPathOverride)
  }

}
