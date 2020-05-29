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

package za.co.absa.enceladus.plugins.builtin.controlinfo.mq

import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.plugins.api.control.ControlMetricsPlugin
import za.co.absa.enceladus.plugins.builtin.common.mq.InfoProducer
import za.co.absa.enceladus.plugins.builtin.controlinfo
import za.co.absa.enceladus.plugins.builtin.controlinfo.DceControlInfo

/**
 * Implementation of a plugin that sends control information to a messaging system when
 * a checkpoint is created or when run status is changed.
 */
class ControlInfoSenderPlugin(producer: InfoProducer[DceControlInfo]) extends ControlMetricsPlugin {
  val DatasetNameParam = "datasetName"
  val DatasetVersionParam = "datasetVersion"
  val ReportDateParam = "reportDate"
  val ReportVersionParam = "reportVersion"
  val RunStatusParam = "runStatus"

  override def onCheckpoint(measurements: ControlMeasure, params: Map[String, String]): Unit = {
    val dceControlInfo = controlinfo.DceControlInfo(params(DatasetNameParam),
      params(DatasetVersionParam).toInt,
      params(ReportDateParam),
      params(ReportVersionParam).toInt,
      params(RunStatusParam),
      measurements)
    producer.send(dceControlInfo)
  }
  override def close(): Unit = producer.close()
}
