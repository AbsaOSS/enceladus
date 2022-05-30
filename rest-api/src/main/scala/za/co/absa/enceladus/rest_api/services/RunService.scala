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

package za.co.absa.enceladus.rest_api.services

import java.util.UUID

import com.mongodb.MongoWriteException
import org.joda.time.format.DateTimeFormat
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.rest_api.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.rest_api.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary, TodaysRunsStatistics}
import za.co.absa.enceladus.rest_api.repositories.RunMongoRepository
import za.co.absa.enceladus.model.{Run, SplineReference, Validation}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@Service("runService") // by-name qualifier: making V2 autowiring un-ambiguous
class RunService @Autowired()(val mongoRepository: RunMongoRepository)
  extends ModelService[Run] {

  protected val runMongoRepository: RunMongoRepository = mongoRepository // alias

  def getGroupedRunSummariesPerDatasetName(): Future[Seq[RunDatasetNameGroupedSummary]] = {
    runMongoRepository.getGroupedRunSummariesPerDatasetName()
  }

  def getGroupedRunSummariesPerDatasetVersion(datasetName: String): Future[Seq[RunDatasetVersionGroupedSummary]] = {
    runMongoRepository.getGroupedRunSummariesPerDatasetVersion(datasetName)
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  import za.co.absa.enceladus.model.Validation._

  def getAllLatest(): Future[Seq[Run]] = {
    runMongoRepository.getAllLatest()
  }

  def getCount(): Future[Long] = {
    runMongoRepository.count()
  }

  def getTodaysRunsStatistics(): Future[TodaysRunsStatistics] = {
    for {
      total <- runMongoRepository.getTodaysRuns()
      successfulWithErrors <- runMongoRepository.getTodaysSuccessWithErrors()
      successfulAll <- runMongoRepository.getTodaysSuccessfulRuns()
      running <- runMongoRepository.getTodaysRunningRuns()
      stdSuccessful <- runMongoRepository.getTodaysStdSuccessRuns()
      failed <- runMongoRepository.getTodaysFailedRuns()
    } yield TodaysRunsStatistics(total, failed, successfulAll - successfulWithErrors, successfulWithErrors, running, stdSuccessful)
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    Try(DateTimeFormat.forPattern("dd-MM-yyyy").parseLocalDate(startDate)) match {
      case _: Success[_] => runMongoRepository.getByStartDate(startDate)
      case _: Failure[_] =>
        val validation = Validation().withError("startDate", s"must have format dd-MM-yyyy: $startDate")
        throw ValidationException(validation)
    }
  }

  def getAllSummaries(): Future[Seq[RunSummary]] = {
    runMongoRepository.getAllSummaries()
  }

  def getSummariesByDatasetName(datasetName: String): Future[Seq[RunSummary]] = {
    runMongoRepository.getSummariesByDatasetName(datasetName)
  }

  def getSummariesByDatasetNameAndVersion(datasetName: String, datasetVersion: Int): Future[Seq[RunSummary]] = {
    runMongoRepository.getSummariesByDatasetNameAndVersion(datasetName, datasetVersion)
  }

  def getRunBySparkAppId(appId: String): Future[Seq[Run]] = {
    runMongoRepository.getRunBySparkAppId(appId)
  }

  def getRunByUniqueId(uniqueId: String): Future[Option[Run]] = {
    runMongoRepository.getByUniqueId(uniqueId)
  }

  def getRun(datasetName: String, datasetVersion: Int, runId: Int): Future[Run] = {
    runMongoRepository.getRun(datasetName, datasetVersion, runId).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def getLatestRun(datasetName: String, datasetVersion: Int): Future[Run] = {
    runMongoRepository.getLatestRun(datasetName, datasetVersion).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def create(newRun: Run, username: String, retriesLeft: Int = 3): Future[Run] = {
    for {
      latestOpt  <- runMongoRepository.getLatestRun(newRun.dataset, newRun.datasetVersion)
      run        <- getRunIdentifiersIfAbsent(newRun, username, latestOpt) // adds uniqueId, replaces runId
      validation <- validate(run)
      createdRun <-
        if (validation.isValid) {
          super.create(run)
            .recoverWith {
              case e: MongoWriteException =>
                log.warn("Failed to create Run", e)
                if (retriesLeft > 0) {
                  log.warn(s"Retries left: $retriesLeft")
                  log.warn(s"Retrying to create Run: $newRun")
                  create(newRun, username, retriesLeft - 1)
                } else {
                  throw ValidationException(validation.withError("runId", s"run with this runId already exists: ${run.runId}"))
                }
          }
        } else {
          log.warn(s"Validation failed for Run: $validation")
          throw ValidationException(validation)
        }
    } yield createdRun
  }

  def addCheckpoint(uniqueId: String, checkpoint: Checkpoint): Future[Run] = {
    runMongoRepository.appendCheckpoint(uniqueId, checkpoint).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def addCheckpoint(datasetName: String, datasetVersion: Int, runId: Int, newCheckpoint: Checkpoint): Future[Run] = {
    runMongoRepository.appendCheckpoint(datasetName, datasetVersion, runId, newCheckpoint).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def updateControlMeasure(uniqueId: String, controlMeasure: ControlMeasure): Future[Run] = {
    runMongoRepository.updateControlMeasure(uniqueId, controlMeasure).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def updateSplineReference(uniqueId: String, splineReference: SplineReference): Future[Run] = {
    runMongoRepository.updateSplineReference(uniqueId, splineReference).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def updateRunStatus(uniqueId: String, runStatus: RunStatus): Future[Run] = {
    runMongoRepository.updateRunStatus(uniqueId, runStatus).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def updateRunStatus(datasetName: String, datasetVersion: Int, runId: Int, newRunStatus: RunStatus): Future[Run] = {
    runMongoRepository.updateRunStatus(datasetName, datasetVersion, runId, newRunStatus).map {
      case Some(run) => run
      case None      => throw NotFoundException()
    }
  }

  def validate(run: Run): Future[Validation] = {
    validateUniqueId(run).map { validation =>
      validateDatasetName(run, validation)
    }.map { validation =>
      validateDatasetVersion(run, validation)
    }
  }

  private def validateDatasetVersion(run: Run, validation: Validation): Validation = {
    val datasetVersionOpt = Option(run.datasetVersion)

    if (datasetVersionOpt.contains(0)) {
      validation.withError("datasetVersion", NotSpecified)
    } else {
      validation
    }
  }

  private def validateDatasetName(run: Run, validation: Validation) = {
    if (Option(run.dataset).isEmpty) {
      validation.withError("dataset", NotSpecified)
    } else {
      validation
    }
  }

  protected def validateUniqueId(run: Run): Future[Validation] = {
    val validation = Validation()

    run.uniqueId match {
      case Some(uniqueId) => validateUniqueness(validation, uniqueId)
      case None           =>
        Future.successful(validation.withError("uniqueId", NotSpecified))
    }
  }

  private def validateUniqueness(validation: Validation, uniqueId: String): Future[Validation] = {
    runMongoRepository.existsId(uniqueId).map {
      case true  => validation.withError("uniqueId", s"run with this uniqueId already exists: $uniqueId")
      case false => validation
    }
  }


  private def getNewRunId(latestOpt: Option[Run]): Int = {
    latestOpt match {
      case Some(latest) => latest.runId + 1
      case None         => 1
    }
  }

  private def getUniqueIdIfAbsent(newRun: Run): String = {
    newRun.uniqueId match {
      case Some(id) => id
      case None     => UUID.randomUUID().toString
    }
  }

  private def getRunIdentifiersIfAbsent(newRun: Run, username: String, latestOpt: Option[Run]): Future[Run] = {
    Future.successful {
      val uniqueId = getUniqueIdIfAbsent(newRun)
      val runId = getNewRunId(latestOpt)
      newRun.copy(uniqueId = Option(uniqueId), runId = runId)
    }
  }

}
