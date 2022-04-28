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
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository}
import za.co.absa.enceladus.rest_api.services.{MappingTableService, SchemaService, VersionedModelService}

import scala.concurrent.Future

@Service
class MappingTableServiceV3 @Autowired()(mappingTableMongoRepository: MappingTableMongoRepository,
                                         datasetMongoRepository: DatasetMongoRepository,
                                         val schemaService: SchemaServiceV3)
  extends MappingTableService(mappingTableMongoRepository, datasetMongoRepository) with HavingSchemaService {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def validate(item: MappingTable): Future[Validation] = {
    for {
      originalValidation <- super.validate(item)
      mtSchemaValidation <- validateSchemaExists(item.schemaName, item.schemaVersion)
    } yield originalValidation.merge(mtSchemaValidation)

  }
}
