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

package za.co.absa.enceladus.common.plugin

import java.time.Instant

import com.typesafe.config.Config
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.enceladus.plugins.api.postprocessor.PostProcessorPluginParams.ErrorSourceId._
import za.co.absa.enceladus.plugins.api.postprocessor.{PostProcessor, PostProcessorPluginParams}
import za.co.absa.enceladus.plugins.builtin.errorinfo.mq.ErrorInfoSenderPlugin

object PostProcessingService {
  //scalastyle:off parameter.number
  def forStandardization(config: Config,
                         datasetName: String,
                         datasetVersion: Int,
                         reportDate: String,
                         reportVersion: Int,
                         outputPath: String,
                         sourceSystem: String,
                         runUrls: Option[String],
                         runId: Option[Int],
                         uniqueRunId: Option[String],
                         processingTimestamp: Instant): PostProcessingService = {
    val params = PostProcessorPluginParams(datasetName, datasetVersion, reportDate, reportVersion, outputPath,
      Standardization, sourceSystem, runUrls, runId, uniqueRunId, processingTimestamp)
    PostProcessingService(config, params)
  }

  def forConformance(config: Config,
                     datasetName: String,
                     datasetVersion: Int,
                     reportDate: String,
                     reportVersion: Int,
                     outputPath: String,
                     sourceSystem: String,
                     runUrls: Option[String],
                     runId: Option[Int],
                     uniqueRunId: Option[String],
                     processingTimestamp: Instant): PostProcessingService = {
    val params = PostProcessorPluginParams(datasetName, datasetVersion, reportDate, reportVersion, outputPath,
      Conformance, sourceSystem, runUrls, runId, uniqueRunId, processingTimestamp)
    PostProcessingService(config, params)
    //scalastyle:on parameter.number
  }

}

case class PostProcessingService private(config: Config,
                                         additionalParams: PostProcessorPluginParams) {

  private val log = LogManager.getLogger(classOf[ErrorInfoSenderPlugin])

  log.info(s"PostProcessingService initialized with config=$config and additionalParams=$additionalParams")

  private val postProcessorPluginKey = additionalParams.sourceId match {
    case Standardization => "standardization.plugin.postprocessor"
    case Conformance => "conformance.plugin.postprocessor"
  }

  private val postProcessingPlugins: Seq[PostProcessor] = new PluginLoader[PostProcessor].loadPlugins(config, postProcessorPluginKey)
  /** Called when a dataset is saved. */
  def onSaveOutput(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    postProcessingPlugins.foreach { plugin =>
      plugin.onDataReady(dataFrame, additionalParams)
    }
  }

}
