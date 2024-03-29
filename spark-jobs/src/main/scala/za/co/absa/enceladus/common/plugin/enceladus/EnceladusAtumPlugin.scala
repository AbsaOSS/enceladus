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

package za.co.absa.enceladus.common.plugin.enceladus

import com.typesafe.config.Config
import za.co.absa.atum.plugins.PluginManager
import za.co.absa.enceladus.dao.EnceladusDAO

/**
  * This is Enceladus plugin for Conformance Framework
  */
object EnceladusAtumPlugin {

  private var listener: Option[EventListenerEnceladus] = None

  /**
    * This is Enceladus plugin for Conformance Framework
    *
    * @param config         A configuration of the application plugins could use to init themselves.
    * @param datasetName    The name of the Dataset
    * @param datasetVersion The version of the Dataset
    * @param isJobStageOnly true if the Spark job is only a stage of some job chain
    * @param generateNewRun true if a new run needs to be generated for the Spark job
    */
  def enableEnceladusAtumPlugin(config: Config,
                                datasetName: String,
                                datasetVersion: Int,
                                reportDate: String,
                                reportVersion: Int,
                                isJobStageOnly: Boolean = false,
                                generateNewRun: Boolean = false)
                               (implicit dao: EnceladusDAO): Unit = {
    val eventListener = new EventListenerEnceladus(config,
      dao,
      datasetName,
      datasetVersion,
      reportDate,
      reportVersion,
      isJobStageOnly,
      generateNewRun)
    listener = Option(eventListener)
    PluginManager.loadPlugin(eventListener)
  }

  def runUniqueId: Option[String] = listener.flatMap(_.runUniqueId)

  def runNumber: Option[Int] = listener.flatMap(_.runNumber)

}
