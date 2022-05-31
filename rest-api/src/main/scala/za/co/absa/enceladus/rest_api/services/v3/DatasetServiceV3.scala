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
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{Dataset, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.exceptions.ValidationException
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, OozieRepository}
import za.co.absa.enceladus.rest_api.services.DatasetService._
import za.co.absa.enceladus.rest_api.services.DatasetService

import scala.concurrent.Future

// this DatasetService is a V3 difference wrapper - once V2 is removed, implementations can/should be merged
@Service
class DatasetServiceV3 @Autowired()(datasetMongoRepository: DatasetMongoRepository,
                                    oozieRepository: OozieRepository,
                                    propertyDefinitionService: PropertyDefinitionServiceV3,
                                    mappingTableService: MappingTableServiceV3,
                                    val schemaService: SchemaServiceV3)
  extends DatasetService(datasetMongoRepository, oozieRepository, propertyDefinitionService)
  with HavingSchemaService with VersionedModelServiceV3[Dataset] {

  import scala.concurrent.ExecutionContext.Implicits.global

  def validateRules(item: Dataset): Future[Validation] =  {
    val validationsFutList: Seq[Future[Validation]] = item.conformance.map {
      case r: MappingConformanceRule =>
        mappingTableService.getVersion(r.mappingTable, r.mappingTableVersion).map {
          case Some(_) => Validation.empty //MT exists
          case None => Validation.empty.withError("mapping-table", s"Mapping table ${r.mappingTable} v${r.mappingTableVersion} not found!")
        }
      case _ => Future.successful(Validation.empty) // no other validations besides mapping CRs
    }

    Future.sequence(validationsFutList).map { listOfVals =>
      listOfVals.foldLeft(Validation.empty)(_ merge _)
    }
  }

  // general entity validation is extendable for V3 - here with properties validation
  override def validate(item: Dataset): Future[Validation] = {
    // individual validations are deliberately not run in parallel - the latter may not be needed if the former fails
    for {
      originalValidation <- super.validate(item)
      propertiesValidation <- validateProperties(item.propertiesAsMap)
      schemaValidation <- validateSchemaExists(item.schemaName, item.schemaVersion)
      rulesValidation <- validateRules(item)
    } yield originalValidation.merge(propertiesValidation).merge(schemaValidation).merge(rulesValidation)
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

  def updateProperties(username: String, datasetName: String, datasetVersion: Int,
                       updatedProperties: Map[String, String]): Future[Option[(Dataset, Validation)]] = {
    for {
      successfulValidation <- validateProperties(updatedProperties).flatMap {
        case validation if !validation.isValid => Future.failed(ValidationException(validation)) // warnings are ok for update
        case validation => Future.successful(validation) // empty or with warnings
      }

      // updateFuture includes latest-check and version increase
      update <- updateFuture(username, datasetName, datasetVersion) { latest =>
        Future.successful(latest.copy(properties = Some(removeBlankProperties(updatedProperties))))
      }
    } yield update
  }

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    super.getUsedIn(name, version).map(_.normalized)
  }

}


