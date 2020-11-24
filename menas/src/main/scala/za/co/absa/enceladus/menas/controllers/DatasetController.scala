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

package za.co.absa.enceladus.menas.controllers

import java.net.URI
import java.util.concurrent.CompletableFuture

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.menas.services.DatasetService

@RestController
@RequestMapping(path = Array("/api/dataset"))
class DatasetController @Autowired()(datasetService: DatasetService)
  extends VersionedModelController(datasetService) {

  import za.co.absa.enceladus.menas.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(Array("/{datasetName}/rule/create"))
  @ResponseStatus(HttpStatus.OK)
  def addConformanceRule(@AuthenticationPrincipal user: UserDetails,
                         @PathVariable datasetName: String,
                         @RequestBody rule: ConformanceRule): CompletableFuture[Dataset] = {
    //TODO: we need to figure out how to deal with versioning properly from UX perspective
    for {
      latestVersion <- datasetService.getLatestVersionValue(datasetName)
      res <- latestVersion match {
        case Some(version) => datasetService.addConformanceRule(user.getUsername, datasetName, version, rule).map {
          case Some(ds) => ds
          case _        => throw notFound()
        }
        case _ => throw notFound()
      }
    } yield res
  }


  @GetMapping(Array("/{datasetName}/{datasetVersion}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def getAllProperties(@PathVariable datasetName: String,
                       @PathVariable datasetVersion: Int): CompletableFuture[Map[String, String]] = {
    datasetService.getVersion(datasetName, datasetVersion).map {
      case Some(entity) => entity.propertiesAsMap
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{datasetName}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def getAllProperties(@PathVariable datasetName: String): CompletableFuture[Map[String, String]] = {
    datasetService.getLatestVersion(datasetName).map {
      case Some(entity) => entity.propertiesAsMap
      case None => throw notFound()
    }
  }

  @PutMapping(Array("/{datasetName}/properties"))
  @ResponseStatus(HttpStatus.CREATED)
  def replaceProperties(@AuthenticationPrincipal principal: UserDetails,
                        @PathVariable datasetName: String,
                        @RequestBody newProperties: Map[String, String]): CompletableFuture[ResponseEntity[Option[Dataset]]] = {

    datasetService.replaceProperties(principal.getUsername, datasetName, newProperties).map { dataset =>
      dataset match {
        case None => throw notFound()
        case Some(ds) => {
          val location: URI = new URI(s"/api/dataset/${ds.name}/${ds.version}")
          ResponseEntity.created(location).body(dataset)
        }
      }
    }
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/properties/valid"))
  @ResponseStatus(HttpStatus.OK)
  def getPropertiesValidation(@PathVariable datasetName: String, @PathVariable datasetVersion: Int): CompletableFuture[Validation] = {
    datasetService.getVersion(datasetName, datasetVersion).flatMap {
      case Some(entity) => datasetService.validateProperties(entity.propertiesAsMap)
      case None => throw notFound()
    }
  }

}
