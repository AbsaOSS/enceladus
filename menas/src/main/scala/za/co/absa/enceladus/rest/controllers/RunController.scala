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

package za.co.absa.enceladus.rest.controllers

import java.util.concurrent.CompletableFuture

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation._
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.rest.services.RunService

import scala.concurrent.Future

@RestController
@RequestMapping(path = Array("/api/runs"))
class RunController @Autowired()(runService: RunService) extends BaseController {

  import za.co.absa.enceladus.rest.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  // Dummy implementation for #86
  // TODO: Implementation in #85
  @GetMapping(Array("/list"))
  @ResponseStatus(HttpStatus.OK)
  def list(): CompletableFuture[Seq[Run]] = {
    runService.getAllLatest()
  }

  @GetMapping(Array("/startDate/{startDate}"))
  def findByStartDate(): CompletableFuture[List[Run]] = Future(List())

  @GetMapping(Array("/splineUrl/{datasetName}/{datsetVersion}/{runId}"))
  def getSplineUrl(): CompletableFuture[String] = Future("")

  @GetMapping(Array("/{datasetName}/{datasetVersion}/latest"))
  def getLatestRun(): CompletableFuture[Run] = Future(null)

  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}"))
  def getRun(): CompletableFuture[Run] = Future(null)

  @PostMapping(Array("/create"))
  def create(): CompletableFuture[Run] = Future(null)

  @PostMapping(Array("/addCheckpoint/{uniqueId}"))
  def addCheckpoint(): CompletableFuture[Checkpoint] = Future(null)

  @PostMapping(Array("/updateControlMeasure/{uniqueId}"))
  def updateControlMeasure(): CompletableFuture[ControlMeasure] = Future(null)

  @PostMapping(Array("/updateSplineReference/{uniqueId}"))
  def updateSplineReference(): CompletableFuture[SplineReference] = Future(null)

  @PostMapping(Array("/updateRunStatus/{uniqueId}"))
  def updateRunStatus(): CompletableFuture[RunStatus] = Future(null)

}
