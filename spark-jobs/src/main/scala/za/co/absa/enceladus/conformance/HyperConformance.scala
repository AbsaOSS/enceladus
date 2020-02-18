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

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.common.Constants._
import za.co.absa.enceladus.conformance.interpreter.{Always, DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.{MenasCredentialsFactory, MenasKerberosCredentialsFactory, MenasPlainCredentialsFactory}
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}


class HyperConformance (implicit cmd: ConfCmdConfig,
                        featureSwitches: FeatureSwitches,
                        menasBaseUrls: List[String]) extends StreamTransformer {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[IllegalArgumentException]
  def transform(streamData: DataFrame): DataFrame = {
    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

    implicit val spark: SparkSession = streamData.sparkSession
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()

    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)
    dao.authenticate()

    val reportVersion = getReportVersion

    logPreConformanceInfo(streamData)

    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion)

    DynamicInterpreter.interpret(conformance, streamData)
      .withColumnIfDoesNotExist(InfoDateColumn, to_date(lit(cmd.reportDate), ReportDateFormat))
      .withColumnIfDoesNotExist(InfoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(reportVersion))
  }

  private def logPreConformanceInfo(streamData: DataFrame): Unit = {
    log.info(s"Menas URLs: ${menasBaseUrls.mkString(",")}, dataset=${cmd.datasetName}, version=${cmd.datasetVersion}")
    log.info(s"Input schema: ${streamData.schema.prettyJson}")
  }

  @throws[IllegalArgumentException]
  private def getReportVersion(implicit cmd: ConfCmdConfig): Int = {
    cmd.reportVersion match {
      case Some(version) => version
      case None => throw new IllegalArgumentException("Report version is not provided.")
    }
  }
}

/**
 * This is the definition of Dynamic Conformance as a component of Hyperdrive.
 *
 * In order to use it in hyperdrive the component needs to be configured in 'ingestion.properties' as follows:
 * {{{
 * transformer.hyperconformance.menas.rest.uri=http://localhost:8080
 * transformer.hyperconformance.dataset.name=example
 * transformer.hyperconformance.dataset.version=1
 * transformer.hyperconformance.report.date=2020-01-29
 * transformer.hyperconformance.report.version=1
 *
 * # Either plain credentials
 * transformer.hyperconformance.menas.credentials.file=/path/menas.credentials
 *
 * # Or a keytab
 * transformer.hyperconformance.menas.auth.keytab=/path/to/keytab
 * }}}
 */
object HyperConformance extends StreamTransformerFactory {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  // Configuration keys expected to be set up when running Conformance as a Transformer component for Hyperdrive
  val menasUriKey = "transformer.hyperconformance.menas.rest.uri"
  val menasCredentialsFileKey = "transformer.hyperconformance.menas.credentials.file"
  val menasAuthKeytabKey = "transformer.hyperconformance.menas.auth.keytab"
  val datasetNameKey = "transformer.hyperconformance.dataset.name"
  val datasetVersionKey = "transformer.hyperconformance.dataset.version"
  val reportDateKey = "transformer.hyperconformance.report.date"
  val reportVersionKey = "transformer.hyperconformance.report.version"

  @throws[IllegalArgumentException]
  override def apply(conf: Configuration): StreamTransformer = {
    log.info("Building HyperConformance")

    validateConfiguration(conf)

    val menasCredentialsFactory = getMenasCredentialsFactory(conf: Configuration)

    implicit val cmd: ConfCmdConfig = ConfCmdConfig(
      datasetName = conf.getString(datasetNameKey),
      datasetVersion = conf.getInt(datasetVersionKey),
      reportDate = conf.getString(reportDateKey),
      reportVersion = Option(conf.getInt(reportVersionKey)),
      menasCredentialsFactory = menasCredentialsFactory,
      performanceMetricsFile = None,
      publishPathOverride = None,
      folderPrefix = None,
      experimentalMappingRule = Some(true),
      isCatalystWorkaroundEnabled = Some(true),
      autocleanStandardizedFolder = Some(false),
      persistStorageLevel = None
    )

    implicit val featureSwitcher: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(true)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)
      .setBroadcastStrategyMode(Always)
      .setBroadcastMaxSizeMb(0)

    implicit val menasBaseUrls: List[String] = MenasConnectionStringParser.parse(conf.getString(menasUriKey))

    new HyperConformance()
  }

  @throws[IllegalArgumentException]
  def validateConfiguration(conf: Configuration): Unit = {
    val mandatoryKeys = List(menasUriKey, datasetNameKey, datasetVersionKey, reportDateKey, reportVersionKey)

    val missingKeys = mandatoryKeys.filterNot(key => conf.containsKey(key))

    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Missing mandatory configuration parameters for keys: ${missingKeys.mkString(", ")}.")
    }
  }

  @throws[IllegalArgumentException]
  private def getMenasCredentialsFactory(conf: Configuration): MenasCredentialsFactory = {
    val hasCredentialsFile = conf.containsKey(menasCredentialsFileKey)
    val hasKeytab = conf.containsKey(menasAuthKeytabKey)

    (hasCredentialsFile, hasKeytab) match {
      case (false, false) => throw new IllegalArgumentException("No authentication method is specified.")
      case (true, false)  => new MenasPlainCredentialsFactory(conf.getString(menasCredentialsFileKey))
      case (false, true)  => new MenasKerberosCredentialsFactory(conf.getString(menasCredentialsFileKey))
      case (true, true)   => throw new IllegalArgumentException("Either a credentials file or a keytab should be specified, but not both.")
    }
  }

}

