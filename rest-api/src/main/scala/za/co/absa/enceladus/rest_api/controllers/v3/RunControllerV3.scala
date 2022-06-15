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

package za.co.absa.enceladus.rest_api.controllers.v3

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.servlet.support.ServletUriComponentsBuilder
import za.co.absa.atum.model.{Checkpoint, ControlMeasureMetadata, RunStatus}
import za.co.absa.enceladus.model.{Run, Validation}
import za.co.absa.enceladus.rest_api.controllers.BaseController
import za.co.absa.enceladus.rest_api.controllers.v3.RunControllerV3.LatestKey
import za.co.absa.enceladus.rest_api.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.services.v3.RunServiceV3

import java.net.URI
import java.util.Optional
import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object RunControllerV3 {
  final val LatestKey = "latest"
}

@RestController
@RequestMapping(path = Array("/api-v3/runs"), produces = Array("application/json"))
class RunControllerV3 @Autowired()(runService: RunServiceV3) extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @GetMapping()
  @ResponseStatus(HttpStatus.OK)
  def list(@RequestParam startDate: Optional[String],
           @RequestParam sparkAppId: Optional[String],
           @RequestParam uniqueId: Optional[String]
          ): CompletableFuture[Seq[RunSummary]] = {
    require(Seq(startDate, sparkAppId, uniqueId).count(_.isPresent) <= 1,
      "You may only supply one of [startDate|sparkAppId|uniqueId].")

    runService.getLatestOfEachRunSummary(
      startDate = startDate.toScalaOption,
      sparkAppId = sparkAppId.toScalaOption,
      uniqueId = uniqueId.toScalaOption
    )
    // todo pagination #2060
  }

  // todo pagination #2060
  @GetMapping(Array("/{datasetName}"))
  @ResponseStatus(HttpStatus.OK)
  def getSummariesByDatasetName(@PathVariable datasetName: String,
                                @RequestParam startDate: Optional[String]): CompletableFuture[Seq[RunSummary]] = {
    runService.getLatestOfEachRunSummary(
      datasetName = Some(datasetName),
      startDate = startDate.toScalaOption
    )
  }

  // todo pagination #2060
  @GetMapping(Array("/{datasetName}/{datasetVersion}"))
  @ResponseStatus(HttpStatus.OK)
  def getSummariesByDatasetNameAndVersion(@PathVariable datasetName: String,
                                          @PathVariable datasetVersion: Int,
                                          @RequestParam startDate: Optional[String]): CompletableFuture[Seq[RunSummary]] = {
    runService.getRunSummaries(Some(datasetName), Some(datasetVersion), startDate.toScalaOption)
  }

  @PostMapping(Array("/{datasetName}/{datasetVersion}"))
  @ResponseStatus(HttpStatus.CREATED)
  def create(
              @PathVariable datasetName: String,
              @PathVariable datasetVersion: Int,
              @RequestBody run: Run,
              @AuthenticationPrincipal principal: UserDetails,
              request: HttpServletRequest): CompletableFuture[ResponseEntity[String]] = {
    val createdRunFuture = if (datasetName != run.dataset) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity name mismatch: '$datasetName' != '${run.dataset}'"))
    } else if (datasetVersion != run.datasetVersion) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity version mismatch: $datasetVersion != ${run.datasetVersion}"))
    } else {
      runService.create(run, principal.getUsername)
    }

    createdRunFuture.map { createdRun =>
      val location: URI = ServletUriComponentsBuilder.fromRequest(request)
        .path("/{runId}")
        .buildAndExpand(createdRun.runId.toString)
        .toUri
      ResponseEntity.created(location).body(s"Run ${createdRun.runId} with for dataset '$datasetName' v$datasetVersion created.")
    }
  }

  // todo pagination #2060
  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}"))
  @ResponseStatus(HttpStatus.OK)
  def getRun(@PathVariable datasetName: String,
             @PathVariable datasetVersion: Int,
             @PathVariable runId: String): CompletableFuture[Run] = {
    getRunForRunIdExpression(datasetName, datasetVersion, runId) // runId support latest for GET
  }

  @PutMapping(Array("/{datasetName}/{datasetVersion}/{runId}"))
  @ResponseStatus(HttpStatus.OK)
  def updateRunStatus(
                       @PathVariable datasetName: String,
                       @PathVariable datasetVersion: Int,
                       @PathVariable runId: Int,
                       @RequestBody newRunStatus: RunStatus): CompletableFuture[ResponseEntity[String]] = {
    if (newRunStatus.status == null) {
      Future.failed(new IllegalArgumentException("Invalid empty RunStatus submitted"))
    } else {
      runService.updateRunStatus(datasetName, datasetVersion, runId, newRunStatus).map(_ =>
        ResponseEntity.ok(s"New runStatus $newRunStatus applied.")
      )
    }
  }

  // todo pagination #2060 ???
  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}/checkpoints"))
  @ResponseStatus(HttpStatus.OK)
  def getRunCheckpoints(@PathVariable datasetName: String,
                        @PathVariable datasetVersion: Int,
                        @PathVariable runId: String): CompletableFuture[Seq[Checkpoint]] = {
    getRunForRunIdExpression(datasetName, datasetVersion, runId).map(_.controlMeasure.checkpoints)
  }

  @PostMapping(Array("/{datasetName}/{datasetVersion}/{runId}/checkpoints"))
  @ResponseStatus(HttpStatus.CREATED)
  def addCheckpoint(
                     @PathVariable datasetName: String,
                     @PathVariable datasetVersion: Int,
                     @PathVariable runId: Int,
                     @RequestBody newCheckpoint: Checkpoint,
                     request: HttpServletRequest): CompletableFuture[ResponseEntity[String]] = {
    runService.addCheckpoint(datasetName, datasetVersion, runId, newCheckpoint).map { _ =>
        val location: URI = ServletUriComponentsBuilder.fromRequest(request)
          .path("/{cpName}")
          .buildAndExpand(newCheckpoint.name)
          .toUri
        ResponseEntity.created(location).body(s"Checkpoint '${newCheckpoint.name}' added.")
    }
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}/checkpoints/{checkpointName}"))
  @ResponseStatus(HttpStatus.OK)
  def getRunCheckpointByName(@PathVariable datasetName: String,
                             @PathVariable datasetVersion: Int,
                             @PathVariable runId: String,
                             @PathVariable checkpointName: String): CompletableFuture[Checkpoint] = {
    for {
      checkpoints <- getRunForRunIdExpression(datasetName, datasetVersion, runId).map(_.controlMeasure.checkpoints)
      cpByName = checkpoints.find(_.name == checkpointName).getOrElse(throw NotFoundException())
    } yield cpByName
  }

  // todo pagination #2060 ???
  @GetMapping(Array("/{datasetName}/{datasetVersion}/{runId}/metadata"))
  @ResponseStatus(HttpStatus.OK)
  def getRunMetadata(@PathVariable datasetName: String,
                     @PathVariable datasetVersion: Int,
                     @PathVariable runId: String): CompletableFuture[ControlMeasureMetadata] = {
    getRunForRunIdExpression(datasetName, datasetVersion, runId).map(_.controlMeasure.metadata)
  }

  /**
   * Retrieves a Run by dataset name, version and runId (either a number of 'latest')
   *
   * @param datasetName    dataset name
   * @param datasetVersion dateset version
   * @param runIdStr       runId (either a number of 'latest')
   * @return Run object
   */
  protected def getRunForRunIdExpression(datasetName: String, datasetVersion: Int, runIdStr: String): Future[Run] = {
    runIdStr match {
      case LatestKey => runService.getLatestRun(datasetName, datasetVersion)
      case nonLatestRunIdString => Try(nonLatestRunIdString.toInt) match {
        case Success(actualRunId) => runService.getRun(datasetName, datasetVersion, actualRunId)
        case Failure(exception) =>
          Future.failed(new IllegalArgumentException(s"Cannot convert '$runIdStr' to a valid runId expression. " +
            s"Either use 'latest' or an actual runId number. Underlying problem: ${exception.getMessage}"))
      }
    }
  }

}
