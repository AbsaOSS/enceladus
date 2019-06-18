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

package za.co.absa.enceladus.dao.menasplugin

import za.co.absa.atum.plugins.PluginManager
import za.co.absa.enceladus.dao.MenasRestDAO

/**
  * This is Menas plugin for Conformance Framework
  */
object MenasPlugin {

  /**
    * This is Menas plugin for Conformance Framework
    *
    * @param datasetName The name of the Dataset
    * @param datasetVersion The version of the Dataset
    * @param isJobStageOnly true if the spark job is only a stage of some job chain
    */
  def enableMenas(datasetName: String = "",
                  datasetVersion: Int = 1,
                  isJobStageOnly: Boolean = false): Unit = {
    val eventListener = new EventListenerMenas(MenasRestDAO, datasetName, datasetVersion, isJobStageOnly)
    PluginManager.loadPlugin(eventListener)
  }
}
