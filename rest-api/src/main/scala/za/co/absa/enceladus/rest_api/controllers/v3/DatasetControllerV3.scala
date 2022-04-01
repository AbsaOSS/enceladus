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

package za.co.absa.enceladus.rest_api.controllers.v3

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.rest_api.services.v3.DatasetServiceV3
import za.co.absa.enceladus.rest_api.utils.implicits._

import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest
import scala.concurrent.ExecutionContext.Implicits.global

@RestController
@RequestMapping(path = Array("/api-v3/datasets"))
class DatasetControllerV3 @Autowired()(datasetService: DatasetServiceV3)
  extends VersionedModelControllerV3(datasetService) {

  @GetMapping(Array("/{name}/{version}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def getAllPropertiesForVersion(@PathVariable name: String,
                                 @PathVariable version: String): CompletableFuture[Map[String, String]] = {
    forVersionExpression(name, version)(datasetService.getVersion).map {
      case Some(entity) => entity.propertiesAsMap
      case None => throw notFound()
    }
  }

  @PutMapping(Array("/{name}/{version}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def updateProperties(@AuthenticationPrincipal principal: UserDetails,
                       @PathVariable name: String,
                       @PathVariable version: String,
                       @RequestBody newProperties: java.util.Map[String, String],
                       request: HttpServletRequest): CompletableFuture[ResponseEntity[Nothing]] = {
    forVersionExpression(name, version) { case (dsName, dsVersion) =>
      datasetService.updateProperties(principal.getUsername, dsName, dsVersion, newProperties.toScalaMap).map {

        case Some(entity) =>
          // stripping last 3 segments (/dsName/dsVersion/properties), instead of /api-v3/dastasets/dsName/dsVersion/properties we want /api-v3/dastasets/dsName/dsVersion/properties
          createdWithNameVersionLocation(entity.name, entity.version, request, stripLastSegments = 3, suffix = "/properties")
        case None => throw notFound()
      }
    }
  }

  // todo putIntoInfoFile switch needed?

  @GetMapping(Array("/{name}/{version}/rules"))
  @ResponseStatus(HttpStatus.OK)
  def getConformanceRules(@PathVariable name: String,
                          @PathVariable version: String): CompletableFuture[Seq[ConformanceRule]] = {
    forVersionExpression(name, version)(datasetService.getVersion).map {
      case Some(entity) => entity.conformance
      case None => throw notFound()
    }
  }

  @PostMapping(Array("/{name}/{version}/rules"))
  @ResponseStatus(HttpStatus.CREATED)
  def addConformanceRule(@AuthenticationPrincipal user: UserDetails,
                         @PathVariable name: String,
                         @PathVariable version: String,
                         @RequestBody rule: ConformanceRule,
                         request: HttpServletRequest): CompletableFuture[ResponseEntity[Nothing]] = {
    forVersionExpression(name, version)(datasetService.getVersion).flatMap {
      case Some(entity) => datasetService.addConformanceRule(user.getUsername, name, entity.version, rule).map {
        case Some(updatedDs) =>
          val addedRuleOrder = updatedDs.conformance.last.order
          createdWithNameVersionLocation(name, updatedDs.version, request, stripLastSegments = 3, // strip: /{name}/{version}/rules
            suffix = s"/rules/$addedRuleOrder")
        case _ => throw notFound()
      }
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{name}/{version}/rules/{order}"))
  @ResponseStatus(HttpStatus.OK)
  def getConformanceRuleByOrder(@PathVariable name: String,
                                @PathVariable version: String,
                                @PathVariable order: Int): CompletableFuture[ConformanceRule] = {
    for {
      optDs <- forVersionExpression(name, version)(datasetService.getVersion)
      ds = optDs.getOrElse(throw notFound())
      rule = ds.conformance.find(_.order == order).getOrElse(throw notFound())
    } yield rule
  }

}


