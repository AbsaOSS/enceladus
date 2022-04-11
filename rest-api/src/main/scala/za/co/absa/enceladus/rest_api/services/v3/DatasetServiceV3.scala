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
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, OozieRepository}
import za.co.absa.enceladus.rest_api.services.{DatasetService, MappingTableService, PropertyDefinitionService, SchemaService}

import scala.concurrent.Future


// this DatasetService is a V3 difference wrapper - once V2 is removed, implementations can/should be merged
@Service
class DatasetServiceV3 @Autowired()(datasetMongoRepository: DatasetMongoRepository,
                                    oozieRepository: OozieRepository,
                                    datasetPropertyDefinitionService: PropertyDefinitionService,
                                    val schemaService: SchemaService)
  extends DatasetService(datasetMongoRepository, oozieRepository, datasetPropertyDefinitionService)
  with HavingSchemaService {

  import scala.concurrent.ExecutionContext.Implicits.global

  // general entity validation is extendable for V3 - here with properties validation
  override def validate(item: Dataset): Future[Validation] = {

    for {
      originalValidation <- super.validate(item)
      propertiesValidation <- validateProperties(item.propertiesAsMap)
      schemaValidation <- validateSchemaExists(item.schemaName, item.schemaVersion)
      // todo validate CR rule existing MT
    } yield originalValidation.merge(propertiesValidation).merge(schemaValidation)
  }

  override def addConformanceRule(username: String, datasetName: String,
                                  datasetVersion: Int, rule: ConformanceRule): Future[Option[(Dataset, Validation)]] = {
    update(username, datasetName, datasetVersion) { dataset =>
      val existingRuleOrders = dataset.conformance.map(_.order).toSet
      if (!existingRuleOrders.contains(rule.order)) {
        dataset.copy(conformance = dataset.conformance :+ rule) // adding the rule
      } else {
        throw new IllegalArgumentException(s"Rule with order ${rule.order} cannot be added, another rule with this order already exists.")
      }
    }
  }

}


