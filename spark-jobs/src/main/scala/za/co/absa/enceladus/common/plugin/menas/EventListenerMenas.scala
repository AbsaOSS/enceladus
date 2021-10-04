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

package za.co.absa.enceladus.common.plugin.menas

import org.apache.log4j.LogManager
import za.co.absa.atum.core.Atum
import za.co.absa.atum.model._
import za.co.absa.atum.plugins.EventListener
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils
import za.co.absa.enceladus.common.plugin.PluginLoader
import za.co.absa.enceladus.dao.{DaoException, MenasDAO}
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.plugins.api.control.ControlMetricsPlugin
import za.co.absa.enceladus.utils.config.ConfigReader

import scala.util.control.NonFatal

/**
  * This is Menas plugin. The plugin listens to Control Framework events and sends information to Menas using REST API.
  */
class EventListenerMenas(config: ConfigReader,
                         dao: MenasDAO,
                         datasetName: String,
                         datasetVersion: Int,
                         reportDate: String,
                         reportVersion: Int,
                         isJobStageOnly: Boolean,
                         generateNewRun: Boolean) extends EventListener {

  private val log = LogManager.getLogger("EventListenerMenas")

  private var _runUniqueId: Option[String] = None
  private var _runNumber: Option[Int] = None
  private var _runStatus: RunStatus = RunStatus(RunState.running, None)
  private var _controlMeasure: Option[ControlMeasure] = None

  private val metricsPluginKey = if (isJobStageOnly) {
    "standardization.plugin.control.metrics"
  } else {
    "conformance.plugin.control.metrics"
  }

  private val controlMetricPlugins: Seq[ControlMetricsPlugin] = new PluginLoader[ControlMetricsPlugin].loadPlugins(config, metricsPluginKey)

  def runUniqueId: Option[String] = _runUniqueId
  def runNumber: Option[Int] = _runNumber
  def runStatus: RunStatus = _runStatus

  /** Called when an _INFO file have been loaded. */
  override def onLoad(sparkApplicationId: String, inputInfoFileName: String, controlMeasure: ControlMeasure): Unit = {
    if (controlMeasure.runUniqueId.isEmpty || (generateNewRun && _runUniqueId.isEmpty)) {
      if (datasetName.isEmpty) {
        throw new IllegalStateException("The Dataset name is not provided, nor a 'runUniqueId' from the previous " +
          s"stage is present in $inputInfoFileName file. Please, provide the dataset name when invoking " +
          "enableControlFrameworkTracking() or make sure 'runUniqueId' field is present in " +
          s"$inputInfoFileName.")
      }
      val splineRef = SplineReference(sparkApplicationId, "")
      val runStatus = RunStatus(RunState.running, None)
      val run = Run(None,
        0,
        datasetName,
        datasetVersion,
        splineRef,
        ControlMeasureUtils.getTimestampAsString,
        runStatus,
        controlMeasure)
      val storedRun = dao.storeNewRunObject(run)
      _runNumber = Option(storedRun.runId)
      _runUniqueId = storedRun.uniqueId
      Atum.setRunUniqueId(_runUniqueId.get)
    } else {
      _runUniqueId = controlMeasure.runUniqueId
      _runStatus = RunStatus(RunState.running, None)
      val storedRun = dao.updateRunStatus(_runUniqueId.get, _runStatus)
      _runNumber = Option(storedRun.runId)
    }
    _controlMeasure = Option(controlMeasure)
  }

  /** Called when a checkpoint has been completed. */
  override def onControlMeasurementsUpdated(controlMeasure: ControlMeasure): Unit = {
    // This approach makes run object correspond to _INFO file. It just replaces previous runs of the same job
    for (uniqueId <- _runUniqueId) {
      try {
        _controlMeasure = Option(controlMeasure)
        dao.updateControlMeasure(uniqueId, controlMeasure)
        notifyPlugins()
      } catch {
        case NonFatal(e) => throw DaoException(s"Unable to update control measurements for a Run object ($uniqueId) in the database", e)
      }
    }
  }

  /** Called when job status changes. */
  override def onJobStatusChange(newStatus: RunStatus): Unit = {
    for (uniqueId <- _runUniqueId if needToSendStatusChange(_runStatus, newStatus)) {
      val statusToSave = if (isJobStageOnly && newStatus.status == RunState.allSucceeded) {
        newStatus.copy(status = RunState.stageSucceeded)
      } else {
        newStatus
      }
      try {
        dao.updateRunStatus(uniqueId, statusToSave)
        notifyPlugins()
      } catch {
        case NonFatal(e) => throw DaoException(s"Unable to update status of a run object ($uniqueId) in the database", e)
      }
    }
    _runStatus = newStatus
  }

  /** Called when a dataset controlled by Control Framework is saved. */
  override def onSaveOutput(sparkApplicationId: String, outputPathFileName: String): Unit = {
    if (_runUniqueId.isEmpty) {
      log.error(s"Unable to append spline reference as uniqueId of the run is not set")
    }
    else {
      val uniqueId = _runUniqueId.get
      val splineReference = SplineReference(sparkApplicationId, outputPathFileName)
      try {
        dao.updateSplineReference(uniqueId, splineReference)
      } catch {
        case NonFatal(e) => throw DaoException(s"Unable to update spline reference for the Run object ($uniqueId) in the database", e)
      }
    }
  }

  override def onApplicationEnd(): Unit = {
    controlMetricPlugins.foreach(plugin => {
      try {
        plugin.close()
      } catch {
        case NonFatal(ex) =>
          val className = plugin.getClass.getName
          log.error(s"A plugin has thrown an exception on close: $className", ex)
      }
    })
  }

  private def needToSendStatusChange(oldStatus: RunStatus, newStatus: RunStatus): Boolean = {
    // Only send an error status if it hasn't been done so already or if the previous error's failed status doesn't have error details
    if (oldStatus.status == RunState.failed && oldStatus.error.isEmpty && newStatus.status == RunState.failed) {
      if (newStatus.error.nonEmpty) {
        val error = newStatus.error.get
        log.warn(s"Attempt to set an error status twice at ${error.step}. ${error.description}")
      }
      false
    } else {
      true
    }
  }

  private def notifyPlugins(): Unit = {
    val additionalParams = Map[String, String]("datasetName" -> datasetName,
      "datasetVersion" -> datasetVersion.toString,
      "reportDate" -> reportDate,
      "reportVersion" -> reportVersion.toString,
      "runStatus" -> _runStatus.status.toString
    )
    _controlMeasure.foreach(measure =>
      controlMetricPlugins.foreach(plugin => {
        try {
          plugin.onCheckpoint(measure, additionalParams)
        } catch {
          case NonFatal(ex) =>
            val className = plugin.getClass.getName
            log.error(s"A plugin has thrown an exception: $className", ex)
        }
      })
    )
  }
}
