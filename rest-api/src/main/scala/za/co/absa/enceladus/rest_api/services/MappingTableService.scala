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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.{DefaultValue, MappingTable, Schema, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository}

import scala.concurrent.Future

@Service
class MappingTableService @Autowired() (val mongoRepository: MappingTableMongoRepository,
    datasetMongoRepository: DatasetMongoRepository) extends VersionedModelService[MappingTable] {

  protected val mappingTableMongoRepository: MappingTableMongoRepository = mongoRepository // alias

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(mappingTableName: String, mappingTableVersion: Option[Int]): Future[UsedIn] = {
    val used = mappingTableVersion match {
      case Some(version) => datasetMongoRepository.containsMappingRuleRefEqual(("mappingTable", mappingTableName),
        ("mappingTableVersion", version))
      case None          => datasetMongoRepository.containsMappingRuleRefEqual(("mappingTable", mappingTableName))
    }

    used.map(refs => UsedIn(Some(refs), None))
  }

  override def create(mt: MappingTable, username: String): Future[Option[(MappingTable, Validation)]] = {
    val mappingTable = MappingTable(name = mt.name,
      description = mt.description,
      schemaName = mt.schemaName,
      schemaVersion = mt.schemaVersion,
      hdfsPath = mt.hdfsPath,
      filter = mt.filter)
    super.create(mappingTable, username)
  }

  def updateDefaults(username: String, mtName: String, mtVersion: Int, defaultValues: List[DefaultValue]): Future[Option[(MappingTable, Validation)]] = {
    super.update(username, mtName, mtVersion) { latest =>
      latest.setDefaultMappingValue(defaultValues)
    }
  }

  def addDefault(username: String, mtName: String, mtVersion: Int, defaultValue: DefaultValue): Future[Option[(MappingTable, Validation)]] = {
    super.update(username, mtName, mtVersion) { latest =>
      latest.setDefaultMappingValue(latest.defaultMappingValue :+ defaultValue)
    }
  }

  override def update(username: String, mt: MappingTable): Future[Option[(MappingTable, Validation)]] = {
    super.update(username, mt.name, mt.version) { latest =>
      latest
        .setHDFSPath(mt.hdfsPath)
        .setSchemaName(mt.schemaName)
        .setSchemaVersion(mt.schemaVersion)
        .setDescription(mt.description).asInstanceOf[MappingTable]
        .setFilter(mt.filter)
    }
  }

  override def importItem(item: MappingTable, username: String): Future[Option[(MappingTable, Validation)]] = {
    getLatestVersionValue(item.name).flatMap {
      case Some(version) => update(username, item.copy(version = version))
      case None => super.create(item.copy(version = 1), username)
    }
  }

  override def validateSingleImport(item: MappingTable, metadata: Map[String, String]): Future[Validation] = {
    val maybeSchema = datasetMongoRepository.getConnectedSchema(item.schemaName, item.schemaVersion)

    val validationBase = super.validateSingleImport(item, metadata)
    val validationSchema = validateSchema(item.schemaName, item.schemaVersion, maybeSchema)
    val validationDefaultValues = validateDefaultValues(item, maybeSchema)
    for {
      base <- validationBase
      schema <- validationSchema
      defaultValues <- validationDefaultValues
    } yield base.merge(schema).merge(defaultValues)
  }

  private def validateDefaultValues(item: MappingTable, maybeSchema: Future[Option[Schema]]): Future[Validation] = {
    maybeSchema.map(schema => {
      val fields = schema match {
        case Some(s) => s.fields.flatMap(f => f.getAllChildrenBasePath :+ f.path).toSet
        case None => Set.empty[String]
      }
      item.defaultMappingValue.foldLeft(Validation()) { (accValidations, defaultValue) =>
        accValidations.withErrorIf(
          !fields.contains(defaultValue.columnName),
          "item.defaultMappingValue",
          s"Cannot find field ${defaultValue.columnName} in schema")
      }
    })
  }
}
