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

package za.co.absa.enceladus.model.test.factories

import java.util.UUID

import za.co.absa.atum.model.RunState.RunState
import za.co.absa.atum.model._
import za.co.absa.enceladus.model.{Run, SplineReference}

object RunFactory {

  val dummyDateString = "04-12-2017 16:19:17 +0200"

  def getDummyRun(uniqueId: Option[String] = Option(UUID.randomUUID().toString),
                  runId: Int = 1,
                  dataset: String = "dummyDataset",
                  datasetVersion: Int = 1,
                  splineRef: SplineReference = getDummySplineReference(),
                  startDateTime: String = dummyDateString,
                  runStatus: RunStatus = getDummyRunStatus(),
                  controlMeasure: ControlMeasure = getDummyControlMeasure()): Run = {
    Run(uniqueId, runId, dataset, datasetVersion, splineRef, startDateTime, runStatus, controlMeasure)
  }

  def getDummySplineReference(sparkApplicationId: String = "dummySparkApplicationId",
                              outputPath: String = "dummyOutputPath"): SplineReference = {
    SplineReference(sparkApplicationId, outputPath)
  }

  def getDummyRunStatus(runState: RunState = RunState.allSucceeded,
                        error: Option[RunError] = None): RunStatus = {
    RunStatus(runState, error)
  }

  def getDummyRunStatusSuccess(): RunStatus = {
    RunStatus(RunState.allSucceeded, None)
  }

  def getDummyRunStatusError(): RunStatus = {
    RunStatus(RunState.failed, Some(getDummyRunError()))
  }

  def getDummyRunError(job: String = "dummyJob",
                       step: String = "dummyStep",
                       description: String = "dummyDescription",
                       technicalDetails: String = "dummyTechnicalDetails"): RunError = {
    RunError(job, step, description, technicalDetails)
  }

  def getDummyControlMeasure(metadata: ControlMeasureMetadata = getDummyMetadata(),
                             runUniqueId: Option[String] = Option(UUID.randomUUID().toString),
                             checkpoints: List[Checkpoint] = List()): ControlMeasure = {
    ControlMeasure(metadata, runUniqueId, checkpoints)
  }

  def getDummyMetadata(sourceApplication: String = "dummySourceApplication",
                       country: String = "dummyCountry",
                       historyType: String = "dummyHistoryType",
                       dataFilename: String = "dummyDataFilename",
                       sourceType: String = "dummySourceType",
                       version: Int = 1,
                       informationDate: String = dummyDateString,
                       additionalInfo: Map[String, String] = Map()): ControlMeasureMetadata = {
    ControlMeasureMetadata(sourceApplication, country, historyType, dataFilename,
      sourceType, version, informationDate, additionalInfo)
  }

  def getDummyCheckpoint(name: String = "dummyName",
                         processStartTime: String = dummyDateString,
                         processEndTime: String = dummyDateString,
                         workflowName: String = "dummyWorkFlowName",
                         order: Int = 0,
                         controls: List[Measurement] = List()): Checkpoint = {
    Checkpoint(name, processStartTime, processEndTime, workflowName, order, controls)
  }

  def getDummyMeasurement(controlName: String = "dummyControlName",
                          controlType: String = "dummyControlType",
                          controlCol: String = "dummyControlCol",
                          controlValue: Any = 0): Measurement = {
    Measurement(controlName, controlType, controlCol, controlValue)
  }

}
