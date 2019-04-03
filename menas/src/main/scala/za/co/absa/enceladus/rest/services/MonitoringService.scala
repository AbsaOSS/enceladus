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
import org.mongodb.scala.{Document, bson}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.rest.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.rest.models.Validation
import za.co.absa.enceladus.rest.repositories.MonitoringMongoRepository
import za.co.absa.enceladus.rest.models.MonitoringDataPoint

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@Service
class MonitoringService @Autowired()(monitoringMongoRepository: MonitoringMongoRepository)
  extends ModelService(monitoringMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  @Value("${za.co.absa.enceladus.spline.urlTemplate}")
  val splineUrlTemplate: String = null

  def getMonitoringDataPoints(datasetName: String): Future[String] = {
    monitoringMongoRepository.getMonitoringDataPoints(datasetName).map(_.mkString("[", ",", "]"))
  }


}
