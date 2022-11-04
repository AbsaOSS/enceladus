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
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import za.co.absa.enceladus.common.Constants._
import za.co.absa.enceladus.common.version.SparkVersionGuard
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{Always, DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.conformance.streaming.{InfoDateFactory, InfoVersionFactory}
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.dao.OptionallyRetryableException._
import za.co.absa.enceladus.dao.auth.{RestApiCredentialsFactory, RestApiKerberosCredentialsFactory, RestApiPlainCredentialsFactory}
import za.co.absa.enceladus.dao.rest.RestDaoFactory.AvailabilitySetup
import za.co.absa.enceladus.dao.rest.{RestApiConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.model.{ConformedSchema, Dataset}
import za.co.absa.enceladus.utils.fs.HadoopFsUtils
import za.co.absa.enceladus.utils.validation.ValidationLevel
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}

class HyperConformance (restApiBaseUrls: List[String],
                        urlsRetryCount: Option[Int] = None,
                        restApiAvailabilitySetup: Option[String] = None,
                        optionallyRetryableExceptions: Set[OptRetryableExceptions] = Set.empty)
                       (implicit cmd: ConformanceConfig,
                        featureSwitches: FeatureSwitches,
                        infoDateFactory: InfoDateFactory,
                        infoVersionFactory: InfoVersionFactory) extends StreamTransformer {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[IllegalArgumentException]
  def transform(rawDf: DataFrame): DataFrame = {
    implicit val spark: SparkSession = rawDf.sparkSession
    val restApiCredentials = cmd.restApiCredentialsFactory.getInstance()

    val restApiAvailabilitySetupValue = restApiAvailabilitySetup
      .map(AvailabilitySetup.withName).getOrElse(RestDaoFactory.DefaultAvailabilitySetup)
    implicit val dao: EnceladusDAO = RestDaoFactory.getInstance(
      restApiCredentials,
      restApiBaseUrls,
      urlsRetryCount,
      restApiAvailabilitySetupValue,
      optionallyRetryableExceptions)
    dao.authenticate()

    logPreConformanceInfo(rawDf)

    val conformance = dao.getDataset(cmd.datasetName, cmd.datasetVersion, ValidationLevel.ForRun)
    val conformedDf = applyConformanceTransformations(rawDf, conformance)
    log.info(s"Raw schema: ${rawDf.schema.treeString}")
    log.info(s"Publish schema: ${conformedDf.schema.treeString}")
    conformedDf
  }

  def applyConformanceTransformations(rawDf: DataFrame, conformance: Dataset)
                                     (implicit sparkSession: SparkSession, enceladusDAO: EnceladusDAO): DataFrame = {
    import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements

    val schema: StructType = enceladusDAO.getSchema(conformance.schemaName, conformance.schemaVersion)
    val schemaFields = if (schema == null) List() else schema.fields.toList
    val conformedSchema = ConformedSchema(schemaFields, conformance)
    val infoDateColumn = infoDateFactory.getInfoDateColumn(rawDf)
    val infoVersionColumn = infoVersionFactory.getInfoVersionColumn(conformedSchema)

    // using HDFS implementation until HyperConformance is S3-ready
    implicit val hdfs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    implicit val hdfsUtils: HadoopFsUtils = HadoopFsUtils.getOrCreate(hdfs)
    val dataFormat = coalesce(date_format(infoDateColumn, "yyyy-MM-dd"), lit(""))
    val currentDateColumn = current_date()
    import za.co.absa.enceladus.utils.schema.SparkUtils.DataFrameWithEnhancements
    val conformedDf = DynamicInterpreter().interpret(conformance, rawDf)
      .withColumnOverwriteIfExists(InfoDateColumn, coalesce(infoDateColumn, currentDateColumn))
      .withColumnOverwriteIfExists(InfoDateColumnString, dataFormat)
      .withColumnOverwriteIfExists(InfoVersionColumn, infoVersionColumn)
    conformedDf
  }

  private def logPreConformanceInfo(streamData: DataFrame): Unit = {
    log.info(s"REST API URLs: ${restApiBaseUrls.mkString(",")}, dataset=${cmd.datasetName}, version=${cmd.datasetVersion}")
    log.info(s"Input schema: ${streamData.schema.prettyJson}")
  }
}

/**
 * This is the definition of Dynamic Conformance as a component of Hyperdrive.
 *
 * See https://github.com/AbsaOSS/hyperdrive#configuration for instructions how the component needs to be configured in Hyperdrive.
 * Example values are given below:
 * {{{
 * enceladus.rest.uri=http://localhost:8080
 * dataset.name=example
 * dataset.version=1
 * report.date=2020-01-29
 * report.version=1
 * event.timestamp.column=EV_TIME
 *
 * # Either plain credentials
 * enceladus.rest.credentials.file=/path/rest_api.credentials
 *
 * # Or a keytab
 * enceladus.rest.auth.keytab=/path/to/keytab
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

    val restApiCredentialsFactory = getRestApiCredentialsFactory(conf: Configuration)

    implicit val confConfig: ConformanceConfig = ConformanceConfig(publishPathOverride = None,
      experimentalMappingRule = Some(true),
      isCatalystWorkaroundEnabled = Some(true),
      autocleanStandardizedFolder = Some(false),
      datasetName = conf.getString(datasetNameKey),
      datasetVersion = conf.getInt(datasetVersionKey),
      reportDate = new SimpleDateFormat(ReportDateFormat).format(new Date()),  // Still need a report date for mapping table patterns
      reportVersion = Option(getReportVersion(conf)),
      performanceMetricsFile = None,
      folderPrefix = None,
      persistStorageLevel = None,
      restApiCredentialsFactory = restApiCredentialsFactory
    )

    implicit val featureSwitcher: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(true)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)
      .setBroadcastStrategyMode(Always)
      .setBroadcastMaxSizeMb(0)

    implicit val reportDateCol: InfoDateFactory = InfoDateFactory.getFactoryFromConfig(conf)
    implicit val infoVersionCol: InfoVersionFactory = InfoVersionFactory.getFactoryFromConfig(conf)

    val restApiBaseUrls: List[String] = RestApiConnectionStringParser.parse(conf.getString(restApiUriKey))
    val restApiUrlsRetryCount: Option[Int] = if (conf.containsKey(restApiUriRetryCountKey)) {
      Option(conf.getInt(restApiUriRetryCountKey))
    } else {
      None
    }
    val restApiAvailabilitySetup: Option[String] = if (conf.containsKey(restApiAvailabilitySetupKey)) {
      Option(conf.getString(restApiAvailabilitySetupKey))
    } else {
      None
    }
    val optionallyRetryableExceptions: Set[OptRetryableExceptions] =
      if (conf.containsKey(restApiOptionallyRetryableExceptions)) {
        conf.getList(classOf[Int], restApiOptionallyRetryableExceptions)
          .asScala
          .toSet
          .map(getOptionallyRetryableException)
      } else {
        Set.empty
      }
    new HyperConformance(
      restApiBaseUrls, restApiUrlsRetryCount, restApiAvailabilitySetup, optionallyRetryableExceptions
    )
  }

  private def getReportVersion(conf: Configuration): Int = {
    if (conf.containsKey(reportVersionKey)) {
      conf.getInt(reportVersionKey)
    } else {
      defaultReportVersion
    }
  }

  @throws[IllegalArgumentException]
  def validateConfiguration(conf: Configuration): Unit = {
    val mandatoryKeys = List(restApiUriKey, datasetNameKey, datasetVersionKey)

    val missingKeys = mandatoryKeys.filterNot(key => conf.containsKey(key))

    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Missing mandatory configuration parameters for keys: ${missingKeys.mkString(", ")}.")
    }
  }

  @throws[IllegalArgumentException]
  private def getRestApiCredentialsFactory(conf: Configuration): RestApiCredentialsFactory = {
    val hasCredentialsFile = conf.containsKey(restApiCredentialsFileKey)
    val hasKeytab = conf.containsKey(restApiAuthKeytabKey)

    (hasCredentialsFile, hasKeytab) match {
      case (false, false) => throw new IllegalArgumentException("No authentication method is specified.")
      case (true, false)  => new RestApiPlainCredentialsFactory(conf.getString(restApiCredentialsFileKey))
      case (false, true)  => new RestApiKerberosCredentialsFactory(conf.getString(restApiAuthKeytabKey))
      case (true, true)   => throw new IllegalArgumentException("Either a credentials file or a keytab should be specified, but not both.")
    }
  }
}
