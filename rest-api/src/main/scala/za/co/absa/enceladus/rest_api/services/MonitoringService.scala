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

import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest_api.repositories.MonitoringMongoRepository

import scala.concurrent.Future

@Service
class MonitoringService @Autowired()(val mongoRepository: MonitoringMongoRepository)
  extends ModelService[Run] {

  import scala.concurrent.ExecutionContext.Implicits.global

  def getMonitoringDataPoints(datasetName: String, startDate: String, endDate: String): Future[String] = {
    mongoRepository.getMonitoringDataPoints(datasetName, startDate, endDate).map(_.mkString("[", ",", "]"))
  }

}
