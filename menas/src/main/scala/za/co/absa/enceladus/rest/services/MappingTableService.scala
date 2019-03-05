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
import za.co.absa.enceladus.model.{MappingTable, UsedIn}
import za.co.absa.enceladus.rest.repositories.{DatasetMongoRepository, MappingTableMongoRepository}
import scala.concurrent.Future
import za.co.absa.enceladus.model.DefaultValue

@Service
class MappingTableService @Autowired() (mappingTableMongoRepository: MappingTableMongoRepository,
    datasetMongoRepository: DatasetMongoRepository) extends VersionedModelService(mappingTableMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(mappingTableName: String, mappingTableVersion: Option[Int]): Future[UsedIn] = {
    val used = mappingTableVersion match {
      case Some(version) => datasetMongoRepository.containsMappingRuleRefEqual(("mappingTable", mappingTableName), ("mappingTableVersion", version))
      case None          => datasetMongoRepository.containsMappingRuleRefEqual(("mappingTable", mappingTableName))
    }
    
    used.map(refs => UsedIn(Some(refs), None))
  }

  override def create(mt: MappingTable, username: String): Future[Option[MappingTable]] = {
    val mappingTable = MappingTable(name = mt.name,
      description = mt.description,
      schemaName = mt.schemaName,
      schemaVersion = mt.schemaVersion,
      hdfsPath = mt.hdfsPath)
    super.create(mappingTable, username)
  }

  def updateDefaults(username: String, mtName: String, mtVersion: Int, defaultValues: List[DefaultValue]): Future[Option[MappingTable]] = {
    super.update(username, mtName, mtVersion) { latest =>
      latest.setDefaultMappingValue(defaultValues)
    }
  }

  def addDefault(username: String, mtName: String, mtVersion: Int, defaultValue: DefaultValue): Future[Option[MappingTable]] = {
    super.update(username, mtName, mtVersion) { latest =>
      latest.setDefaultMappingValue(latest.defaultMappingValue :+ defaultValue)
    }
  }

  override def update(username: String, mt: MappingTable): Future[Option[MappingTable]] = {
    super.update(username, mt.name, mt.version) { latest =>
      latest
        .setHDFSPath(mt.hdfsPath)
        .setSchemaName(mt.schemaName)
        .setSchemaVersion(mt.schemaVersion)
        .setDescription(mt.description).asInstanceOf[MappingTable]
    }
  }

}
