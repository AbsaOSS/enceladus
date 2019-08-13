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
import java.time.LocalDate
import java.time.format.DateTimeFormatter

@Component
class OozieService @Autowired() (oozieRepository: OozieRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  def isOozieEnabled: Boolean = oozieRepository.isOozieEnabled()

  def getCoordinatorStatus(coordinatorId: String): Future[OozieCoordinatorStatus] = {
    oozieRepository.getCoordinatorStatus(coordinatorId)
  }

  def runNow(oozieSchedule: OozieSchedule, reportDate: Option[String]): Future[String] = {
    val wfPath = oozieSchedule.activeInstance match {
      case Some(instance) => instance.workflowPath
      case None           => throw new IllegalArgumentException("Cannot run a non-active schedule.")
    }
    val reportDateString = reportDate match {
      case Some(date) => date
      case None =>
        val d = LocalDate.now().plusDays(oozieSchedule.reportDateOffset)
        DateTimeFormatter.ofPattern("yyyy-MM-dd").format(d)

    }
    oozieRepository.runWorkflow(wfPath, oozieSchedule.runtimeParams, reportDateString)
  }

  def suspend(coordinatorId: String): Future[OozieCoordinatorStatus] = {
    oozieRepository.suspend(coordinatorId).flatMap(_ => this.getCoordinatorStatus(coordinatorId))
  }

  def resume(coordinatorId: String): Future[OozieCoordinatorStatus] = {
    oozieRepository.resume(coordinatorId).flatMap(_ => this.getCoordinatorStatus(coordinatorId))
  }
}
