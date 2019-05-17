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

package za.co.absa.enceladus.menas.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.concurrent.Future
import scala.util.Try
import za.co.absa.enceladus.menas.repositories.OozieRepository
import za.co.absa.enceladus.model.Dataset
import org.apache.oozie.client.Job.Status
import za.co.absa.enceladus.menas.models.OozieCoordinatorStauts

@Component
class OozieService @Autowired() (oozieRepository: OozieRepository) {
  import scala.concurrent.ExecutionContext.Implicits.global
  
  def isOozieEnabled: Boolean = oozieRepository.isOozieEnabled
  
  def getCoordinatorStatus(coordinatorId: String): Future[OozieCoordinatorStauts] = {
    oozieRepository.getCoordinatorStatus(coordinatorId)
  }
}
