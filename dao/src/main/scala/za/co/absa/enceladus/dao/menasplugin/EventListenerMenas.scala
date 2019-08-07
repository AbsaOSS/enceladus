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

import org.apache.log4j.LogManager
import za.co.absa.atum.core.Atum
import za.co.absa.atum.model._
import za.co.absa.atum.plugins.EventListener
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.dao.{MenasDAO, UnauthorizedException}
import za.co.absa.enceladus.model.{Run, SplineReference}

import scala.util.{Failure, Success}

/**
  * This is Menas plugin. The plugin listens to Control Framework events and sends information to Menas using REST API.
  */
class EventListenerMenas(menasDao: MenasDAO,
                         datasetName: String,
                         datasetVersion: Int,
                         isJobStageOnly: Boolean,
                         generateNewRun: Boolean) extends EventListener {

  private val dao: MenasDAO = menasDao
  private val log = LogManager.getLogger("EventListenerMenas")
  private var runUniqueId: Option[String] = None
  private var runStatus: RunStatus = RunStatus(RunState.running, None)

  /** Called when an _INFO file have been loaded. */
  override def onLoad(sparkApplicationId: String, inputInfoFileName: String, controlMeasure: ControlMeasure): Unit = {
    if (controlMeasure.runUniqueId.isEmpty || (generateNewRun && runUniqueId.isEmpty)) {
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
        ControlUtils.getTimestampAsString,
        runStatus,
        controlMeasure)
      dao.storeNewRunObject(run) match {
        case Success(uniqueId) =>
          runUniqueId = Some(uniqueId)
          Atum.setRunUniqueId(uniqueId)
        case Failure(e) => e match {
          case unAuthException:UnauthorizedException => throw unAuthException
          case _ => log.error(s"Unable to store the Run object for the job, checkpoints will not be saved to database")
      }}
    } else {
      runUniqueId = controlMeasure.runUniqueId
      runStatus = RunStatus(RunState.running, None)
      dao.updateRunStatus(runUniqueId.get, runStatus)
    }
  }

  /** Called when a checkpoint has been completed. */
  override def onControlMeasurementsUpdated(controlMeasure: ControlMeasure): Unit = {
    // This approach makes run object correspond to _INFO file. It just replaces previous runs of the same job
    for (uniqueId <- runUniqueId) {
      if (!dao.updateControlMeasure(uniqueId, controlMeasure)) {
        log.error(s"Unable to update control measurements for a Run object ($uniqueId) in the database")
      }
    }
  }

  /** Called when job status changes. */
  override def onJobStatusChange(newStatus: RunStatus): Unit = {
    for (uniqueId <- runUniqueId if needToSendStatusChange(runStatus, newStatus)) {
      val statusToSave = if (isJobStageOnly && newStatus.status == RunState.allSucceeded) {
        newStatus.copy(status = RunState.stageSucceeded)
      } else {
        newStatus
      }
      if (!dao.updateRunStatus(uniqueId, statusToSave)) {
        log.error(s"Unable to update status of a run object ($uniqueId) in the database")
      }
    }
    runStatus = newStatus
  }

  /** Called when a dataset controlled by Control Framework is saved. */
  override def onSaveOutput(sparkApplicationId: String, outputPathFileName: String): Unit = {
    if (runUniqueId.isEmpty) {
      log.error(s"Unable to append spline reference as uniqueId of the run is not set")
    }
    else {
      val uniqueId = runUniqueId.get
      val splineReference = SplineReference(sparkApplicationId, outputPathFileName)
      if (!dao.updateSplineReference(uniqueId, splineReference)) {
        log.error(s"Unable to update spline reference for the Run object ($uniqueId) in the database")
      }
    }
  }

  private def needToSendStatusChange(oldStatus: RunStatus, newStatus: RunStatus): Boolean = {
    // Only send an error status if it hasn't been done so already or if the previous error's failed status doesn't have error details
    if (oldStatus.status == RunState.failed && oldStatus.error.isEmpty && newStatus.status == RunState.failed) {
      if (newStatus.error.nonEmpty) {
        val error = newStatus.error.get
        log.warn(s"Attempt to set an error status twice at ${error.step}. ${error.description}")
      }
      false
    }
    else {
      true
    }
  }
}
