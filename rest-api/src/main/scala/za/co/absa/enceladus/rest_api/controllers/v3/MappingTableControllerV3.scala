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
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.rest_api.services.MappingTableService

import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest

@RestController
@RequestMapping(Array("/api-v3/mapping-tables"))
class MappingTableControllerV3 @Autowired()(mappingTableService: MappingTableService)
  extends VersionedModelControllerV3(mappingTableService) {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @GetMapping(path = Array("/{name}/{version}/defaults"))
  @ResponseStatus(HttpStatus.OK)
  def getDefaults(@PathVariable name: String,
                  @PathVariable version: String): CompletableFuture[Seq[DefaultValue]] = {

    forVersionExpression(name, version)(mappingTableService.getVersion).map { // "latest" version is accepted
      case Some(entity) => entity.defaultMappingValue
      case None => throw notFound()
    }
  }

  @PutMapping(path = Array("/{name}/{version}/defaults"))
  @ResponseStatus(HttpStatus.CREATED)
  def updateDefault(@AuthenticationPrincipal user: UserDetails,
                 @PathVariable name: String,
                 @PathVariable version: String,
                 @RequestBody newDefaults: Array[DefaultValue],
                 request: HttpServletRequest
                ): CompletableFuture[ResponseEntity[Validation]] = {
    for {
      existingMtOpt <- forVersionExpression(name, version)(mappingTableService.getVersion)
      existingMt = existingMtOpt.getOrElse(throw notFound())
      updatedMtAndValidationOpt <- mappingTableService.updateDefaults(user.getUsername, name, existingMt.version, newDefaults.toList)
      (updatedMt, validation) = updatedMtAndValidationOpt.getOrElse(throw notFound())
      response = createdWithNameVersionLocationBuilder(name, updatedMt.version, request,
        stripLastSegments = 3, suffix = s"/defaults").body(validation)  // stripping: /{name}/{version}/defaults
    } yield response
  }

  @PostMapping(path = Array("/{name}/{version}/defaults"))
  @ResponseStatus(HttpStatus.CREATED)
  def addDefault(@AuthenticationPrincipal user: UserDetails,
                 @PathVariable name: String,
                 @PathVariable version: String,
                 @RequestBody newDefault: DefaultValue,
                 request: HttpServletRequest
                ): CompletableFuture[ResponseEntity[Validation]] = {
    for {
      existingMtOpt <- forVersionExpression(name, version)(mappingTableService.getVersion)
      existingMt = existingMtOpt.getOrElse(throw notFound())
      updatedMtAndValidationOpt <- mappingTableService.addDefault(user.getUsername, name, existingMt.version, newDefault)
      (updatedMt, validation) = updatedMtAndValidationOpt.getOrElse(throw notFound())
      response = createdWithNameVersionLocationBuilder(name, updatedMt.version, request,
        stripLastSegments = 3, suffix = s"/defaults").body(validation)  // stripping: /{name}/{version}/defaults
    } yield response
  }

}
