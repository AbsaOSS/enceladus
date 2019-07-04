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

package za.co.absa.enceladus.menas.controllers

import java.util.concurrent.CompletableFuture

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.menas.models.RunSummary
import za.co.absa.enceladus.menas.services.RunService

@RestController
@RequestMapping(path = Array("/api/runs"), produces = Array("application/json"))
class RunController @Autowired()(runService: RunService) extends BaseController {

  import za.co.absa.enceladus.menas.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @GetMapping()
  @ResponseStatus(HttpStatus.OK)
  def list(): CompletableFuture[String] = {
    runService.getAllLatest().map(ControlUtils.asJson)
  }

  @GetMapping(Array("/startDate/{startDate}"))
  @ResponseStatus(HttpStatus.OK)
  def getByStartDate(@PathVariable startDate: String): CompletableFuture[String] = {
    runService.getByStartDate(startDate).map(ControlUtils.asJson)
  }

  @GetMapping(Array("/summaries"))
  @ResponseStatus(HttpStatus.OK)
  def getAllSummaries(): CompletableFuture[Seq[RunSummary]] = {
    runService.getAllSummaries()
  }

  @GetMapping(Array("/{datasetName}"))
  @ResponseStatus(HttpStatus.OK)
  def getSummariesByDatasetName(@PathVariable datasetName: String): CompletableFuture[Seq[RunSummary]] = {
    runService.getSummariesByDatasetName(datasetName)
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}"))
  @ResponseStatus(HttpStatus.OK)
  def getSummariesByDatasetNameAndVersion(@PathVariable datasetName: String,
                                          @PathVariable datasetVersion: Int): CompletableFuture[Seq[RunSummary]] = {
    runService.getSummariesByDatasetNameAndVersion(datasetName, datasetVersion)
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}"))
  @ResponseStatus(HttpStatus.OK)
  def getRun(@PathVariable datasetName: String,
             @PathVariable datasetVersion: Int,
             @PathVariable runId: Int): CompletableFuture[String] = {
    runService.getRun(datasetName, datasetVersion, runId).map(ControlUtils.asJson)
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/latestrun"))
  @ResponseStatus(HttpStatus.OK)
  def getLatestRun(@PathVariable datasetName: String,
                   @PathVariable datasetVersion: Int): CompletableFuture[String] = {
    runService.getLatestRun(datasetName, datasetVersion).map(ControlUtils.asJson)
  }

  @GetMapping(Array("/splineUrl/{datasetName}/{datasetVersion}/{runId}"))
  @ResponseStatus(HttpStatus.OK)
  def getSplineUrl(@PathVariable datasetName: String,
                   @PathVariable datasetVersion: Int,
                   @PathVariable runId: Int): CompletableFuture[String] = {
    runService.getSplineUrl(datasetName, datasetVersion, runId)
  }

  @PostMapping()
  @ResponseStatus(HttpStatus.CREATED)
  def create(@RequestBody run: Run,
             @AuthenticationPrincipal principal: UserDetails): CompletableFuture[Run] = {
    runService.create(run, principal.getUsername)
  }

  @PostMapping(Array("/addCheckpoint/{uniqueId}"))
  @ResponseStatus(HttpStatus.OK)
  def addCheckpoint(@PathVariable uniqueId: String,
                    @RequestBody checkpoint: Checkpoint): CompletableFuture[Run] = {
    runService.addCheckpoint(uniqueId, checkpoint)
  }

  @PostMapping(Array("/updateControlMeasure/{uniqueId}"))
  @ResponseStatus(HttpStatus.OK)
  def updateControlMeasure(@PathVariable uniqueId: String,
                           @RequestBody controlMeasure: ControlMeasure): CompletableFuture[Run] = {
    runService.updateControlMeasure(uniqueId, controlMeasure)
  }

  @PostMapping(Array("/updateSplineReference/{uniqueId}"))
  @ResponseStatus(HttpStatus.OK)
  def updateSplineReference(@PathVariable uniqueId: String,
                            @RequestBody splineReference: SplineReference): CompletableFuture[Run] = {
    runService.updateSplineReference(uniqueId, splineReference)
  }

  @PostMapping(Array("/updateRunStatus/{uniqueId}"))
  @ResponseStatus(HttpStatus.OK)
  def updateRunStatus(@PathVariable uniqueId: String,
                      @RequestBody runStatus: RunStatus): CompletableFuture[Run] = {
    runService.updateRunStatus(uniqueId, runStatus)
  }

}
