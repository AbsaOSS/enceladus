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
import za.co.absa.enceladus.model.{Run, Validation}
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.repositories.v3.RunMongoRepositoryV3
import za.co.absa.enceladus.rest_api.services.RunService

import scala.concurrent.Future

@Service
class RunServiceV3 @Autowired()(override val mongoRepository: RunMongoRepositoryV3, datasetServiceV3: DatasetServiceV3)
  extends RunService(mongoRepository) {

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
                                datasetVersion: Option[Int] = None,
                                startDate: Option[String] = None,
                                sparkAppId: Option[String] = None,
                                uniqueId: Option[String] = None
                               ): Future[Seq[RunSummary]] = {
    mongoRepository.getRunSummariesLatestOfEach(datasetName, datasetVersion, startDate, sparkAppId, uniqueId)
  }

  def getRunSummaries(datasetName: Option[String] = None,
                      datasetVersion: Option[Int] = None,
                      startDate: Option[String] = None): Future[Seq[RunSummary]] = {
    mongoRepository.getRunSummaries(datasetName, datasetVersion, startDate)
  }

}
