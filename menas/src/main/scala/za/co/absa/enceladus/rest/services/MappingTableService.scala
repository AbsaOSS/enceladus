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

package za.co.absa.enceladus.rest.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.{ MappingTable, UsedIn }
import za.co.absa.enceladus.rest.repositories.{ DatasetMongoRepository, MappingTableMongoRepository }

import scala.concurrent.Future
import za.co.absa.enceladus.model.DefaultValue

@Service
class MappingTableService @Autowired() (mappingTableMongoRepository: MappingTableMongoRepository, auditTrailService: AuditTrailService,
                                        datasetService: DatasetService)
  extends VersionedModelService(mappingTableMongoRepository, auditTrailService) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    datasetService.findDatasetsUsingMappingTable(name, version).map(refs => UsedIn(Some(refs), None))
  }

  override def create(mt: MappingTable, username: String): Future[MappingTable] = {
    val mappingTable = MappingTable(name = mt.name, description = mt.description, schemaName = mt.schemaName,
      schemaVersion = mt.schemaVersion, hdfsPath = mt.hdfsPath)

    super.create(mappingTable, username, s"Mapping Table ${mt.name} created.")
  }
  
  def updateDefaults(username: String, mtName: String, mtVersion: Int, defaultValues: List[DefaultValue]): Future[MappingTable] = {
    super.update(username, mtName, mtVersion, s"Default values updated.") { latest =>
       val updated = latest.setDefaultMappingValue(defaultValues)
       ChangedFieldsUpdateTransformResult(updatedEntity = updated, Seq())
    }
  }
  
  def addDefault(username: String, mtName: String, mtVersion: Int, defaultValue: DefaultValue): Future[MappingTable] = {
    super.update(username, mtName, mtVersion, s"Default value for column ${defaultValue.columnName} added.") { latest =>
      val updated = latest.setDefaultMappingValue(latest.defaultMappingValue :+ defaultValue)
      ChangedFieldsUpdateTransformResult(updatedEntity = updated, Seq())
    }
  }

  override def update(username: String, mt: MappingTable): Future[MappingTable] = {
    super.update(username, mt.name, mt.version, s"Mapping Table Updated.") { latest =>
      val updated = latest
        .setHDFSPath(mt.hdfsPath)
        .setSchemaName(mt.schemaName)
        .setSchemaVersion(mt.schemaVersion)
        .setDescription(mt.description).asInstanceOf[MappingTable]
      
      ChangedFieldsUpdateTransformResult(updatedEntity = updated, Seq(
          ChangedField("HDFS Path", mt.hdfsPath, latest.hdfsPath),
          ChangedField("Schema Name", mt.schemaName, latest.schemaName),
          ChangedField("Schema Version", mt.schemaVersion, latest.schemaVersion),
          ChangedField("Description", mt.description, latest.description)
      ))

    }
  }

}
