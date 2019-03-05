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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.model.{Dataset, UsedIn}
import za.co.absa.enceladus.rest.repositories.DatasetMongoRepository
import scala.concurrent.Future
import za.co.absa.enceladus.model.menas.MenasReference

@Service
class DatasetService @Autowired() (datasetMongoRepository: DatasetMongoRepository) extends VersionedModelService(datasetMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def update(username: String, dataset: Dataset): Future[Option[Dataset]] = {
    super.update(username, dataset.name, dataset.version, "Dataset Updated.") { latest =>
      latest
        .setSchemaName(dataset.schemaName)
        .setSchemaVersion(dataset.schemaVersion)
        .setHDFSPath(dataset.hdfsPath)
        .setHDFSPublishPath(dataset.hdfsPublishPath)
        .setConformance(dataset.conformance)
        .setDescription(dataset.description).asInstanceOf[Dataset]
    }
  }

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    Future.successful(UsedIn())
  }

  override def create(newDataset: Dataset, username: String): Future[Option[Dataset]] = {
    val dataset = Dataset(
      name = newDataset.name,
      version = 0,
      description = newDataset.description,
      hdfsPath = newDataset.hdfsPath,
      hdfsPublishPath = newDataset.hdfsPublishPath,
      schemaName = newDataset.schemaName,
      schemaVersion = newDataset.schemaVersion,
      conformance = List())
    super.create(dataset, username, s"Dataset ${newDataset.name} created.")
  }

  def addConformanceRule(username: String, datasetName: String, datasetVersion: Int, rule: ConformanceRule): Future[Option[Dataset]] = {
    super.update(username, datasetName, datasetVersion, s"Conformance rule (${rule.order}) '${rule.outputColumn}' added.") { dataset =>
      dataset.copy(conformance = dataset.conformance :+ rule)
    }
  }

}
