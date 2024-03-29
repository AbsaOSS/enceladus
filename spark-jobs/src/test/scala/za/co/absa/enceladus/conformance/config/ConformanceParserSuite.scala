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

package za.co.absa.enceladus.conformance.config

import org.apache.hadoop.conf.Configuration

import java.time.ZonedDateTime
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.ConformanceExecution
import za.co.absa.enceladus.dao.auth.{RestApiKerberosCredentials, RestApiPlainCredentials}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase

class ConformanceParserSuite extends AnyFunSuite with TZNormalizedSparkTestBase {

  private val year = "2018"
  private val month = "12"
  private val day = "31"
  private val hdfsRawPath = "/bigdatahdfs/datalake/raw/system/feed"
  private val hdfsPublishPath = "/bigdatahdfs/datalake/publish/system/feed"
  private val hdfsPublishPathOverride = "/bigdatahdfs/datalake/publish/system/feed/override"
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
  private val reportVersion = 3
  private val disabled = false
  private val dateDisabled = None
  private val userDisabled = None
  private val locked = None
  private val dateLocked = None
  private val userLocked = None
  private val folderPrefix = s"year=$year/month=$month/day=$day"
  private val infoDateColumn = "enceladus_info_date"
  private val infoVersionColumn = "enceladus_info_version"

  private object TestDynamicConformance extends ConformanceExecution

  implicit val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration

  test("Test credentials file parsing "){
    val credentials = RestApiPlainCredentials.fromFile(restApiCredentialsFile)

    assert(credentials.username == "user")
    assert(credentials.password == "changeme")
  }

  test("Test keytab file parsing "){
    val credentials = RestApiKerberosCredentials.fromFile(keytabPath)

    assert(credentials === restApiKeytab)
  }

  test("Test credentials file config name backwards compatibility (< 3.0.0)") {
    val cmdConfigDeprecated = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-credentials-file", restApiCredentialsFile))
    val cmdConfig = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile))

    val actualPlainRestApiCredentialsDeprecated = cmdConfigDeprecated.restApiCredentialsFactory.getInstance()
    val actualPlainRestApiCredentials = cmdConfig.restApiCredentialsFactory.getInstance()

    assert(actualPlainRestApiCredentialsDeprecated == actualPlainRestApiCredentials)
  }

  test("Test keytab file config name backwards compatibility (< 3.0.0)") {
    val cmdConfigDeprecated = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--menas-auth-keytab", keytabPath))
    val cmdConfig = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-auth-keytab", keytabPath))

    val actualRestApiKerberosDeprecated = cmdConfigDeprecated.restApiCredentialsFactory.getInstance()
    val actualRestApiKerberos = cmdConfig.restApiCredentialsFactory.getInstance()

    assert(actualRestApiKerberosDeprecated == actualRestApiKerberos)
  }

  test("folder-prefix parameter") {
    val cmdConfigNoFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile))

    val actualPlainRestApiCredentials = cmdConfigNoFolderPrefix.restApiCredentialsFactory.getInstance()

    assert(cmdConfigNoFolderPrefix.datasetName === datasetName)
    assert(cmdConfigNoFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigNoFolderPrefix.reportDate === reportDate)
    assert(cmdConfigNoFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigNoFolderPrefix.folderPrefix.isEmpty)
    assert(cmdConfigNoFolderPrefix.publishPathOverride.isEmpty)
    assert(actualPlainRestApiCredentials === restApiCredentials)

    val cmdConfigFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-auth-keytab", keytabPath,
        "--folder-prefix", folderPrefix))

    val actualRestApiKerberosCredentials = cmdConfigFolderPrefix.restApiCredentialsFactory.getInstance()

    assert(cmdConfigFolderPrefix.datasetName === datasetName)
    assert(cmdConfigFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigFolderPrefix.reportDate === reportDate)
    assert(cmdConfigFolderPrefix.reportVersion.get === reportVersion)
    assert(cmdConfigFolderPrefix.folderPrefix.nonEmpty)
    assert(cmdConfigFolderPrefix.folderPrefix.get === folderPrefix)
    assert(cmdConfigFolderPrefix.publishPathOverride.isEmpty)
    assert(actualRestApiKerberosCredentials === restApiKeytab)

    val cmdConfigPublishPathOverrideAndFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride,
        "--folder-prefix", folderPrefix))

    assert(cmdConfigPublishPathOverrideAndFolderPrefix.datasetName === datasetName)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.datasetVersion === datasetVersion)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.reportDate === reportDate)
    assert(cmdConfigPublishPathOverrideAndFolderPrefix.reportVersion.get === reportVersion)
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
      locked,
      dateLocked,
      userLocked,
      List()
    )
    val cmdConfigNoFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile
      ))
    val cmdConfigFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--folder-prefix", folderPrefix))
    val cmdConfigPublishPathOverride = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride))
    val cmdConfigPublishPathOverrideAndFolderPrefix = ConformanceConfig.getFromArguments(
      Array(
        "--dataset-name", datasetName,
        "--dataset-version", datasetVersion.toString,
        "--report-date", reportDate,
        "--report-version", reportVersion.toString,
        "--folder-prefix", folderPrefix,
        "--rest-api-credentials-file", restApiCredentialsFile,
        "--debug-set-publish-path", hdfsPublishPathOverride))
    val publishPathNoFolderPrefix = TestDynamicConformance
      .getPathConfig(cmdConfigNoFolderPrefix, conformanceDataset, cmdConfigNoFolderPrefix.reportVersion.get).publish.path
    assert(publishPathNoFolderPrefix === s"$hdfsPublishPath/$infoDateColumn=$reportDate/$infoVersionColumn=$reportVersion")
    val publishPathFolderPrefix = TestDynamicConformance
      .getPathConfig(cmdConfigFolderPrefix, conformanceDataset, cmdConfigFolderPrefix.reportVersion.get).publish.path
    assert(publishPathFolderPrefix === s"$hdfsPublishPath/$folderPrefix/$infoDateColumn=$reportDate/$infoVersionColumn=$reportVersion")
    val publishPathPublishPathOverride = TestDynamicConformance
      .getPathConfig(cmdConfigPublishPathOverride, conformanceDataset, cmdConfigPublishPathOverride.reportVersion.get)
        .publish.path
    assert(publishPathPublishPathOverride === hdfsPublishPathOverride)

    val publishPathPublishPathOverrideAndFolderPrefix = TestDynamicConformance
      .getPathConfig(cmdConfigPublishPathOverrideAndFolderPrefix,
        conformanceDataset, cmdConfigPublishPathOverrideAndFolderPrefix.reportVersion.get).publish.path
    assert(publishPathPublishPathOverrideAndFolderPrefix === hdfsPublishPathOverride)
  }

}
