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

import scala.concurrent.Future

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import za.co.absa.enceladus.menas.models.OozieCoordinatorStatus
import za.co.absa.enceladus.menas.repositories.OozieRepository
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieSchedule

@Component
class OozieService @Autowired() (oozieRepository: OozieRepository) {

  def isOozieEnabled: Boolean = oozieRepository.isOozieEnabled()

  def getCoordinatorStatus(coordinatorId: String): Future[OozieCoordinatorStatus] = {
    oozieRepository.getCoordinatorStatus(coordinatorId)
  }

  def runNow(oozieSchedule: OozieSchedule): Future[String] = {
    val wfPath = oozieSchedule.activeInstance match {
      case Some(instance) => instance.workflowPath
      case None           => throw new IllegalArgumentException("Cannot run a non-active schedule.")
    }
    oozieRepository.runWorkflow(wfPath, oozieSchedule.runtimeParams)
  }

  def suspend(coordinatorId: String): Future[Unit] = {
    oozieRepository.suspend(coordinatorId)
  }

  def resume(coordinatorId: String): Future[Unit] = {
    oozieRepository.resume(coordinatorId)
  }
}
