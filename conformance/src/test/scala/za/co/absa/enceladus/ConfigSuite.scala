/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus

import java.time.ZonedDateTime

import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.DynamicConformanceJob
import za.co.absa.enceladus.dao.menasplugin.{MenasKerberosCredentials, MenasPlainCredentials}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ConfigSuite extends FunSuite with SparkTestBase {

  private val year = "2018"
  private val month = "12"
  private val day = "31"
  private val dateTokens = Array(year, month, day)
  private val hdfsRawPath = "/bigdatahdfs/datalake/raw/system/feed"
  private val hdfsPublishPath = "/bigdatahdfs/datalake/publish/system/feed"
  private val hdfsPublishPathOverride = "/bigdatahdfs/datalake/publish/system/feed/override"
  private val menasCredentialsFile = "src/test/resources/menas-credentials.conf"
  private val menasCredentials = Some(MenasPlainCredentials.fromFile(menasCredentialsFile))
  private val keytabPath = "src/test/resources/user.keytab.example"
  private val menasKeytab = Some(MenasKerberosCredentials("user@EXAMPLE.COM", keytabPath))
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
  private val reportVersion = 3
  private val disabled = false
  private val dateDisabled = None
  private val userDisabled = None
  private val rawFormat = "parquet"
  private val folderPrefix = s"year=$year/month=$month/day=$day"
  private val infoDateColumn = "enceladus_info_date"
  private val infoVersionColumn = "enceladus_info_version"

  test("folder-prefix parameter") {
    val cmdConfigNoFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile))
    assert(cmdConfigNoFolderPrefix.datasetName === datasetName)
    assert(cmdConfigNoFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigNoFolderPrefix.reportDate === reportDate)
    assert(cmdConfigNoFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigNoFolderPrefix.menasCredentials === menasCredentials)
    assert(cmdConfigNoFolderPrefix.folderPrefix.isEmpty)
    assert(cmdConfigNoFolderPrefix.publishPathOverride.isEmpty)

    val cmdConfigFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-auth-keytab", keytabPath,
        "--folder-prefix", folderPrefix))
    assert(cmdConfigFolderPrefix.datasetName === datasetName)
    assert(cmdConfigFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigFolderPrefix.reportDate === reportDate)
    assert(cmdConfigFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigFolderPrefix.menasCredentials === menasKeytab)
    assert(cmdConfigFolderPrefix.folderPrefix.nonEmpty)
    assert(cmdConfigFolderPrefix.folderPrefix.get === folderPrefix)
    assert(cmdConfigFolderPrefix.publishPathOverride.isEmpty)

    val cmdConfigPublishPathOverride = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--debug-set-publish-path", hdfsPublishPathOverride))
    assert(cmdConfigPublishPathOverride.datasetName === datasetName)
    assert(cmdConfigPublishPathOverride.datasetVersion === datasetVersion)
    assert(cmdConfigPublishPathOverride.reportDate === reportDate)
    assert(cmdConfigPublishPathOverride.reportVersion.get === reportVersion)
    assert(cmdConfigPublishPathOverride.menasCredentials === None)
    assert(cmdConfigPublishPathOverride.folderPrefix.isEmpty)
    assert(cmdConfigPublishPathOverride.publishPathOverride.nonEmpty)
    assert(cmdConfigPublishPathOverride.publishPathOverride.get === hdfsPublishPathOverride)

    val cmdConfigPublishPathOverrideAndFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride,
        "--folder-prefix", folderPrefix))
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.datasetName === datasetName)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.reportDate === reportDate)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.menasCredentials === menasCredentials)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.folderPrefix.nonEmpty)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.folderPrefix.get === folderPrefix)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.publishPathOverride.nonEmpty)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.publishPathOverride.get === hdfsPublishPathOverride)
  }

  test("Test buildPublishPath") {
    val conformanceDataset = Dataset(
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
    val cmdConfigNoFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile
      ))
    val cmdConfigFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--folder-prefix", folderPrefix))
    val cmdConfigPublishPathOverride = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", menasCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride))
    val cmdConfigPublishPathOverrideAndFolderPrefix = CmdConfig.getCmdLineArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--folder-prefix", folderPrefix,
        "--menas-credentials-file", menasCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride))
    val publishPathNoFolderPrefix = DynamicConformanceJob.buildPublishPath(infoDateColumn, infoVersionColumn,
        cmdConfigNoFolderPrefix, conformanceDataset, cmdConfigNoFolderPrefix.reportVersion.get)
    assert(publishPathNoFolderPrefix === s"$hdfsPublishPath/$infoDateColumn=$reportDate/$infoVersionColumn=$reportVersion")
    val publishPathFolderPrefix = DynamicConformanceJob.buildPublishPath(infoDateColumn, infoVersionColumn,
        cmdConfigFolderPrefix, conformanceDataset, cmdConfigFolderPrefix.reportVersion.get)
    assert(publishPathFolderPrefix === s"$hdfsPublishPath/$folderPrefix/$infoDateColumn=$reportDate/$infoVersionColumn=$reportVersion")
    val publishPathPublishPathOverride = DynamicConformanceJob.buildPublishPath(infoDateColumn, infoVersionColumn,
        cmdConfigPublishPathOverride, conformanceDataset, cmdConfigPublishPathOverride.reportVersion.get)
    assert(publishPathPublishPathOverride === hdfsPublishPathOverride)
    val publishPathPublishPathOverrideAndFolderPrefix = DynamicConformanceJob.buildPublishPath(infoDateColumn, infoVersionColumn,
        cmdConfigPublishPathOverrideAndFolderPrefix, conformanceDataset, cmdConfigPublishPathOverrideAndFolderPrefix.reportVersion.get)
    assert(publishPathPublishPathOverrideAndFolderPrefix === hdfsPublishPathOverride)
  }

}
