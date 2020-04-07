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

package za.co.absa.enceladus.menas.services

import scala.concurrent.Future

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import za.co.absa.enceladus.menas.repositories.DatasetMongoRepository
import za.co.absa.enceladus.menas.repositories.OozieRepository
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieScheduleInstance

@Service
class DatasetService @Autowired() (datasetMongoRepository: DatasetMongoRepository, oozieRepository: OozieRepository)
  extends VersionedModelService(datasetMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def update(username: String, dataset: Dataset): Future[Option[Dataset]] = {
    super.updateFuture(username, dataset.name, dataset.version) { latest =>
      updateSchedule(dataset, latest).map({ withSchedule =>
        withSchedule
          .setSchemaName(dataset.schemaName)
          .setSchemaVersion(dataset.schemaVersion)
          .setHDFSPath(dataset.hdfsPath)
          .setHDFSPublishPath(dataset.hdfsPublishPath)
          .setConformance(dataset.conformance)
          .setDescription(dataset.description).asInstanceOf[Dataset]
        }
      )
    }
  }

  private def updateSchedule(newDataset: Dataset, latest: Dataset): Future[Dataset] = {
    if (newDataset.schedule == latest.schedule) {
      Future(latest)
    } else if (newDataset.schedule.isEmpty) {
      Future(latest.setSchedule(None))
    } else {
      val newInstance = for {
        wfPath <- oozieRepository.createWorkflow(newDataset)
        coordPath <- oozieRepository.createCoordinator(newDataset, wfPath)
        coordId <- latest.schedule match {
          case Some(sched) => sched.activeInstance match {
            case Some(instance) =>
              //Note: use the old schedule's runtime params for the kill - we need to impersonate the right user (it might have been updated)
              oozieRepository.killCoordinator(instance.coordinatorId, sched.runtimeParams).flatMap({ res =>
                oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
              }).recoverWith({
                case ex =>
                  logger.warn("First attempt to kill previous coordinator failed, submitting a new one.")
                  oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
              })
            case None => oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
          }
          case None => oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
        }
      } yield OozieScheduleInstance(wfPath, coordPath, coordId)

      newInstance.map({ i =>
        val schedule = newDataset.schedule.get.copy(activeInstance = Some(i))
        latest.setSchedule(Some(schedule))
      })
    }
  }

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    Future.successful(UsedIn())
  }

  override def create(newDataset: Dataset, username: String): Future[Option[Dataset]] = {
    val dataset = Dataset(name = newDataset.name,
      description = newDataset.description,
      hdfsPath = newDataset.hdfsPath,
      hdfsPublishPath = newDataset.hdfsPublishPath,
      schemaName = newDataset.schemaName,
      schemaVersion = newDataset.schemaVersion,
      conformance = List())
    super.create(dataset, username)
  }

  def addConformanceRule(username: String, datasetName: String, datasetVersion: Int, rule: ConformanceRule): Future[Option[Dataset]] = {
    super.update(username, datasetName, datasetVersion) { dataset =>
      dataset.copy(conformance = dataset.conformance :+ rule)
    }
  }

}
