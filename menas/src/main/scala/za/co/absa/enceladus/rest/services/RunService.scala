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

package za.co.absa.enceladus.rest.services

import java.util.UUID

import org.joda.time.format.DateTimeFormat
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.rest.models.Validation
import za.co.absa.enceladus.rest.repositories.RunMongoRepository

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@Service
class RunService @Autowired()(runMongoRepository: RunMongoRepository)
  extends ModelService(runMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  @Value("${za.co.absa.enceladus.spline.urlTemplate}")
  val splineUrlTemplate: String = null

  def getAllLatest(): Future[Seq[Run]] = {
    runMongoRepository.getAllLatest()
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    Try(DateTimeFormat.forPattern("dd-MM-yyyy").parseLocalDate(startDate)) match {
      case _: Success[_] => runMongoRepository.getByStartDate(startDate)
      case _: Failure[_] =>
        val validation = Validation().withError("startDate", s"must have format dd-MM-yyyy: $startDate")
        throw ValidationException(validation)
    }
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

  def getSplineUrl(datasetName: String, datasetVersion: Int, runId: Int): Future[String] = {
    getRun(datasetName, datasetVersion, runId).map { run =>
      val splineRef = run.splineRef
      String.format(splineUrlTemplate, splineRef.outputPath, splineRef.sparkApplicationId)
    }
  }

  def create(newRun: Run, username: String): Future[Run] = {
    val uniqueId = newRun.uniqueId match {
      case Some(id) => id
      case None     => UUID.randomUUID().toString
    }
    val run = newRun.copy(username = username, uniqueId = Some(uniqueId))
    super.create(run).map(_ => run)
  }

}
