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
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.{UsedIn, Validation}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, PropertyDefinitionMongoRepository}
import za.co.absa.enceladus.rest_api.services.PropertyDefinitionService

import scala.concurrent.Future

@Service
class PropertyDefinitionServiceV3 @Autowired()(propertyDefMongoRepository: PropertyDefinitionMongoRepository,
                                               datasetMongoRepository: DatasetMongoRepository)
  extends PropertyDefinitionService(propertyDefMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    for {
      usedInD <- datasetMongoRepository.findRefContainedAsKey("properties", name)
    } yield UsedIn(Some(usedInD), Some(Seq.empty))
  }

}
