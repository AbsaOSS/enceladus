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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.configuration2.Configuration
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.common.Constants._
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.conformance.interpreter.{Always, DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.conformance.streaming.InfoDateFactory
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.{MenasCredentialsFactory, MenasKerberosCredentialsFactory, MenasPlainCredentialsFactory}
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}

class HyperConformance (implicit cmd: ConfCmdConfig,
                        featureSwitches: FeatureSwitches,
                        menasBaseUrls: List[String],
                        infoDateFactory: InfoDateFactory) extends StreamTransformer {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[IllegalArgumentException]
  def transform(rawDf: DataFrame): DataFrame = {
    implicit val spark: SparkSession = rawDf.sparkSession
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()

    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)
    dao.authenticate()

    logPreConformanceInfo(rawDf)

    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion)
    val conformedDf = applyConformanceTransformations(rawDf, conformance)
    conformedDf
  }

  def applyConformanceTransformations(rawDf: DataFrame, conformance: Dataset)
                                     (implicit sparkSession: SparkSession, menasDAO: MenasDAO): DataFrame = {
    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
    val reportVersion = getReportVersion

    val infoDateColumn = infoDateFactory.getInfoDateColumn(rawDf)

    val conformedDf = DynamicInterpreter.interpret(conformance, rawDf)
      .withColumnIfDoesNotExist(InfoDateColumn, infoDateColumn)
      .withColumnIfDoesNotExist(InfoDateColumnString, date_format(infoDateColumn, "yyyy-MM-dd"))
      .withColumnIfDoesNotExist(InfoVersionColumn, lit(reportVersion))
    conformedDf
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
 * transformer.hyperconformance.event.timestamp.column=EV_TIME
 *
 * # Either plain credentials
 * transformer.hyperconformance.menas.credentials.file=/path/menas.credentials
 *
 * # Or a keytab
 * transformer.hyperconformance.menas.auth.keytab=/path/to/keytab
 * }}}
 */
object HyperConformance extends StreamTransformerFactory with HyperConformanceAttributes {
  import HyperConformanceAttributes._

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val defaultReportVersion = 1

  @throws[IllegalArgumentException]
  override def apply(conf: Configuration): StreamTransformer = {
    log.info("Building HyperConformance")

    SparkVersionGuard.fromDefaultSparkCompatibilitySettings.ensureSparkVersionCompatibility(SPARK_VERSION)

    validateConfiguration(conf)

    val menasCredentialsFactory = getMenasCredentialsFactory(conf: Configuration)

    implicit val cmd: ConfCmdConfig = ConfCmdConfig(
      datasetName = conf.getString(datasetNameKey),
      datasetVersion = conf.getInt(datasetVersionKey),
      reportDate = new SimpleDateFormat(ReportDateFormat).format(new Date()), // Still need a report date for mapping table patterns
      reportVersion = Option(getReportVersion(conf)),
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

    implicit val reportDateCol: InfoDateFactory = InfoDateFactory.getFactoryFromConfig(conf)

    implicit val menasBaseUrls: List[String] = MenasConnectionStringParser.parse(conf.getString(menasUriKey))
    new HyperConformance()
  }

  @throws[IllegalArgumentException]
  def validateConfiguration(conf: Configuration): Unit = {
    val mandatoryKeys = List(menasUriKey, datasetNameKey, datasetVersionKey)

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
      case (false, true)  => new MenasKerberosCredentialsFactory(conf.getString(menasAuthKeytabKey))
      case (true, true)   => throw new IllegalArgumentException("Either a credentials file or a keytab should be specified, but not both.")
    }
  }

  private def getReportVersion(conf: Configuration): Int = {
    if (conf.containsKey(reportVersionKey)) {
      conf.getInt(reportVersionKey)
    } else {
      defaultReportVersion
    }
  }
}

