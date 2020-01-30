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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, to_date}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.conformance.interpreter.{Always, DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.menasplugin.{MenasCredentialsFactory, MenasKerberosCredentials, MenasKerberosCredentialsFactory, MenasPlainCredentials, MenasPlainCredentialsFactory}
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}

import scala.util.control.NonFatal

class HyperConformance (implicit cmd: ConfCmdConfig,
                        featureSwitches: FeatureSwitches,
                        menasBaseUrls: List[String]) extends StreamTransformer {
  private val infoDateColumn = "enceladus_info_date"
  private val infoDateColumnString = s"${infoDateColumn}_string"
  private val reportDateFormat = "yyyy-MM-dd"
  private val infoVersionColumn = "enceladus_info_version"

  def transform(streamData: DataFrame): DataFrame = {
    import HyperConformanceImpl.log

    implicit val spark: SparkSession = streamData.sparkSession
    val menasCredentials = cmd.menasCredentialsFactory.getInstance()

    implicit val dao: MenasDAO = RestDaoFactory.getInstance(menasCredentials, menasBaseUrls)

    dao.authenticate()

    // ToDo - fix this
    val reportVersion = cmd.reportVersion.get

    log.info(s"URL: ${menasBaseUrls.mkString(",")}, dataset=${cmd.datasetName}, version=${cmd.datasetVersion}")
    log.info(s"Schema: ${streamData.schema.prettyJson}")

    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion)

    val result = DynamicInterpreter.interpret(conformance, streamData)

    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
    result
      .withColumnIfDoesNotExist(infoDateColumn, to_date(lit(cmd.reportDate), reportDateFormat))
      .withColumnIfDoesNotExist(infoDateColumnString, lit(cmd.reportDate))
      .withColumnIfDoesNotExist(infoVersionColumn, lit(reportVersion))
  }
}

object HyperConformanceImpl extends StreamTransformerFactory {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def apply(conf: Configuration): StreamTransformer = {
    log.info("Building HyperConformance")

    val menasCredentialsFactory = getMenasCredentialsFactory(conf: Configuration)

    implicit val cmd: ConfCmdConfig = ConfCmdConfig(
      datasetName = conf.getString("dataset.name"),
      datasetVersion = conf.getInt("dataset.version"),
      reportDate = conf.getString("report.date"),
      reportVersion = Option(conf.getInt("report.version")),
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

    implicit val menasBaseUrls: List[String] = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))

    new HyperConformance()
  }

  def getMenasCredentialsFactory(conf: Configuration): MenasCredentialsFactory = {
    val menasCredentialsFileKey = "menas.credentials.file"
    val menasAuthKeytabKey = "menas.auth.keytab"

    val hasCredentialsFile = conf.containsKey(menasCredentialsFileKey)
    val hasKeytab = conf.containsKey(menasAuthKeytabKey)

    (hasCredentialsFile, hasKeytab) match {
      case (false, false) => throw new IllegalStateException("No authentication method is specified.")
      case (true, false)  => new MenasPlainCredentialsFactory(conf.getString(menasCredentialsFileKey))
      case (false, true)  => new MenasKerberosCredentialsFactory(conf.getString(menasCredentialsFileKey))
      case (true, true)   => throw new IllegalStateException("Either a credentials file or a keytab should be specified, but not both.")
    }
  }
}

