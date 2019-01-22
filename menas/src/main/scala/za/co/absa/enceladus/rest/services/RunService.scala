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

import org.joda.time.format.DateTimeFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest.exceptions.ValidationException
import za.co.absa.enceladus.rest.models.Validation
import za.co.absa.enceladus.rest.repositories.RunMongoRepository

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@Service
class RunService @Autowired()(runMongoRepository: RunMongoRepository)
  extends ModelService(runMongoRepository) {

  def getAllLatest(): Future[Seq[Run]] = {
    runMongoRepository.getAllLatest()
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    Try(DateTimeFormat.forPattern("dd-MM-yyyy").parseLocalDate(startDate)) match {
      case Success(x) => runMongoRepository.getByStartDate(startDate)
      case Failure(e) => throw ValidationException(Validation(Map("startDate" -> List(s"must have format dd-MM-yyyy: $startDate"))))
    }
  }

}
