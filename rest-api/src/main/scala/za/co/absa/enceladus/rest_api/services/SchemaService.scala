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

import org.apache.spark.sql.types.StructType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.{Schema, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository, SchemaMongoRepository}
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor

import scala.concurrent.Future

@Service
class SchemaService @Autowired() (schemaMongoRepository: SchemaMongoRepository,
    mappingTableMongoRepository: MappingTableMongoRepository,
    datasetMongoRepository: DatasetMongoRepository,
    sparkMenasConvertor: SparkMenasSchemaConvertor) extends VersionedModelService(schemaMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    for {
      usedInD <- datasetMongoRepository.findRefEqual("schemaName", "schemaVersion", name, version)
      usedInM <- mappingTableMongoRepository.findRefEqual("schemaName", "schemaVersion", name, version)
    } yield UsedIn(Some(usedInD), Some(usedInM))
  }

  def schemaUpload(username: String, schemaName: String, schemaVersion: Int, fields: StructType): Future[(Schema, Validation)] = {
    super.update(username, schemaName, schemaVersion)({ oldSchema =>
      oldSchema.copy(fields = sparkMenasConvertor.convertSparkToMenasFields(fields.fields).toList)
    }).map(_.getOrElse(throw new IllegalArgumentException("Failed to derive new schema from file!")))
  }

  override def recreate(username: String, schema: Schema): Future[Option[(Schema, Validation)]] = {
    for {
      latestVersion <- getLatestVersionNumber(schema.name)
      update <- super.update(username, schema.name, latestVersion) { latest =>
        latest
          .copy(fields = List())
          .setDescription(schema.description).asInstanceOf[Schema]
      }
    } yield update
  }

  /**
   * This method applies only certain fields from `updateSchema` to the subject of this method. Here, for V2 API,
   * only description field is applied, all other fields are disregarded - internally called at create/update
   * @param current existing latest schema prior to changes
   * @param update schema with create/update fields information
   * @return
   */
  protected def updateFields(current: Schema, update: Schema) : Schema = {
    current.setDescription(update.description).asInstanceOf[Schema]
  }

  /** final - override `updateFields` if needed */
  final override def update(username: String, update: Schema): Future[Option[(Schema, Validation)]] = {
    super.update(username, update.name, update.version) { latest =>
      updateFields(latest, update)
    }
  }

  /** final - override `updateFields` if needed */
  final override def create(newSchema: Schema, username: String): Future[Option[(Schema, Validation)]] = {
    val initSchema = Schema(name = newSchema.name, description = newSchema.description)

    val schema = updateFields(initSchema, newSchema)
    super.create(schema, username)
  }

  override def importItem(item: Schema, username: String): Future[Option[(Schema, Validation)]] = {
    getLatestVersionValue(item.name).flatMap {
      case Some(version) => update(username, item.copy(version = version))
      case None => super.create(item.copy(version = 1), username)
    }
  }
}
