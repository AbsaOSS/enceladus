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

package za.co.absa.enceladus.rest_api.services.v3

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.atum.model.Checkpoint
import za.co.absa.enceladus.model.{Run, Validation}
import za.co.absa.enceladus.rest_api.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.repositories.v3.RunMongoRepositoryV3
import za.co.absa.enceladus.rest_api.services.RunService

import java.time.LocalDate
import scala.concurrent.Future

@Service
class RunServiceV3 @Autowired()(runMongoRepository: RunMongoRepositoryV3, datasetServiceV3: DatasetServiceV3)
  extends RunService(runMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def validate(run: Run): Future[Validation] = {
    for {
      uniqueness <- validateUniqueId(run)
      dsExistence <- validateDatasetExists(run.dataset, run.datasetVersion)
    } yield uniqueness.merge(dsExistence)
  }

  protected def validateDatasetExists(datasetName: String, datasetVersion: Int): Future[Validation] = {
    datasetServiceV3.getVersion(datasetName, datasetVersion).map {
      case None => Validation.empty.withError("dataset", s"Dataset $datasetName v$datasetVersion not found!")
      case Some(_) => Validation.empty
    }
  }

  /**
   * Yields Latest-of-each run summaries (grouped by datasetName, datasetVersion).
   * Optionally filtered by one of `startDate` (>=)|`sparkAppId`(==)|`uniqueId`(==)
   * The result is ordered by datasetName, datasetVersion (both ascending)
   * @param startDate
   * @param sparkAppId
   * @param uniqueId
   * @return
   */
  def getLatestOfEachRunSummary(datasetName: Option[String] = None,
                                startDate: Option[LocalDate] = None,
                                sparkAppId: Option[String] = None,
                                uniqueId: Option[String] = None,
                                offset: Option[Int],
                                limit: Option[Int]
                               ): Future[Seq[RunSummary]] = {
    datasetName match {
      case None => runMongoRepository.getRunSummariesLatestOfEach(None, None, startDate, sparkAppId, uniqueId, offset, limit)
      case definedDsName @ Some(dsName) => datasetServiceV3.getLatestVersion(dsName).flatMap {
        case None => Future.failed(NotFoundException(s"Dataset $datasetName at all."))
        case Some(_) => runMongoRepository.getRunSummariesLatestOfEach(definedDsName, None, startDate, sparkAppId, uniqueId, offset, limit)

      }
    }
  }

  def getRunSummaries(datasetName: String,
                      datasetVersion: Int,
                      startDate: Option[LocalDate] = None,
                      offset: Option[Int],
                      limit: Option[Int]): Future[Seq[RunSummary]] = {
    datasetServiceV3.getVersion(datasetName, datasetVersion).flatMap {
      case Some(_) => runMongoRepository.getRunSummaries(Some(datasetName), Some(datasetVersion), startDate, offset, limit)
      case _ => Future.failed(NotFoundException(s"Dataset $datasetName v$datasetVersion does not exist."))
    }
  }

  override def addCheckpoint(datasetName: String, datasetVersion: Int, runId: Int, newCheckpoint: Checkpoint): Future[Run] = {
    // validating for non-duplicate cp name:
    for {
      optRun <- runMongoRepository.getRun(datasetName, datasetVersion, runId)
      run = optRun.getOrElse(throw NotFoundException())
      duplicateExists = run.controlMeasure.checkpoints.find(_.name == newCheckpoint.name).nonEmpty
      _ = if (duplicateExists) {
        throw ValidationException(Validation.empty.withError("checkpoint.name",
          s"Checkpoint with name ${newCheckpoint.name} already exists!"))
      }
      run <- super.addCheckpoint(datasetName, datasetVersion, runId, newCheckpoint)
    } yield run
  }

}
