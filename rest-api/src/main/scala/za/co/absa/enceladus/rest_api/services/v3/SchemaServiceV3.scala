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

package za.co.absa.enceladus.rest_api.services.v3

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.model.{Schema, SchemaField, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository, SchemaMongoRepository}
import za.co.absa.enceladus.rest_api.services.SchemaService
import za.co.absa.enceladus.rest_api.utils.converters.SparkEnceladusSchemaConvertor

import scala.concurrent.Future

@Service
class SchemaServiceV3 @Autowired()(schemaMongoRepository: SchemaMongoRepository,
                                   mappingTableMongoRepository: MappingTableMongoRepository,
                                   datasetMongoRepository: DatasetMongoRepository,
                                   sparkEnceladusConvertor: SparkEnceladusSchemaConvertor)
  extends SchemaService(schemaMongoRepository, mappingTableMongoRepository, datasetMongoRepository, sparkEnceladusConvertor)
  with VersionedModelServiceV3[Schema]{

  import scala.concurrent.ExecutionContext.Implicits.global

  override def validate(item: Schema): Future[Validation] = {
    for {
      originalValidation <- super.validate(item)
      fieldsValidation <- validateSchemaFields(item.fields)
    } yield originalValidation.merge(fieldsValidation)

  }

  protected def validateSchemaFields(fields: Seq[SchemaField]): Future[Validation] = {
    if (fields.isEmpty) {
      // V3 disallows empty schema fields - V2 allowed it at first that to get updated by an attachment upload/remote-load
      Future.successful(Validation.empty.withError("schema-fields","No fields found! There must be fields defined for actual usage."))
    } else {
      Future.successful(Validation.empty)
    }
  }

  // V3 applies fields on create/update from the payload, too (V2 did not allow fields payload here, only via 'upload'
  override protected def updateFields(current: Schema, update: Schema) : Schema = {
    current.setDescription(update.description).asInstanceOf[Schema].copy(fields = update.fields)
  }

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    super.getUsedIn(name, version).map(_.normalized)
  }

}
