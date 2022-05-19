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
import org.springframework.stereotype.Component
import za.co.absa.enceladus.model.properties.{PropertyDefinition, PropertyDefinitionStats}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Component
class StatisticsService @Autowired() (propertyDefService: PropertyDefinitionService, datasetService: DatasetService){
  //#TODO find optimizations #1897
  def getPropertiesWithMissingCount(): Future[Seq[PropertyDefinitionStats]] = {
    val propertyDefsFuture = propertyDefService.getLatestVersions()
    propertyDefsFuture
      .map { (props: Seq[PropertyDefinition]) =>
        val propertiesWithMissingCounts: Seq[Future[PropertyDefinitionStats]] = props.map(propertyDef =>
          datasetService
            .getLatestVersions(Some(propertyDef.name))
            .map(datasetsMissingProp =>
              PropertyDefinitionStats(propertyDef, datasetsMissingProp.size))
        )
         propertiesWithMissingCounts
      }
      .flatMap { propertiesWithMissingCounts: Seq[Future[PropertyDefinitionStats]] =>
        Future.sequence(propertiesWithMissingCounts)
      }
  }

}
