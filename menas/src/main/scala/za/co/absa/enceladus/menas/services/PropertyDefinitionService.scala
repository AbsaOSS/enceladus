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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.menas.repositories.{DatasetMongoRepository, PropertyDefinitionMongoRepository}
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.properties.PropertyDefinition

import scala.concurrent.Future

@Service
class PropertyDefinitionService @Autowired()(propertyDefMongoRepository: PropertyDefinitionMongoRepository)
  extends VersionedModelService(propertyDefMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = Future.successful(UsedIn())

  override def update(username: String, propertyDef: PropertyDefinition): Future[Option[PropertyDefinition]] = {
    super.update(username, propertyDef.name, propertyDef.version) { latest =>
      latest
        .setPropertyType(propertyDef.propertyType)
        .setPutIntoInfoFile(propertyDef.putIntoInfoFile)
        .setEssentiality(propertyDef.essentiality)
        .setDescription(propertyDef.description)
    }
  }

  override def create(newPropertyDef: PropertyDefinition, username: String): Future[Option[PropertyDefinition]] = {
    val propertyDefBase = PropertyDefinition(
      name = newPropertyDef.name,
      description = newPropertyDef.description,
      propertyType = newPropertyDef.propertyType,
      putIntoInfoFile = newPropertyDef.putIntoInfoFile
    ) // has default essentiality

    // if essentiality is not given, apply default:
    val propertyDefinition = if (newPropertyDef.essentiality == null) {
      propertyDefBase // keep the default essentiality
    } else {
      propertyDefBase.copy(essentiality = newPropertyDef.essentiality)
    }

    super.create(propertyDefinition, username)
  }

  override private[services] def importItem(item: PropertyDefinition, username: String): Future[Option[PropertyDefinition]] = {
    getLatestVersionValue(item.name).flatMap {
      case Some(version) => update(username, item.copy(version = version))
      case None => super.create(item.copy(version = 1), username)
    }
  }
}
