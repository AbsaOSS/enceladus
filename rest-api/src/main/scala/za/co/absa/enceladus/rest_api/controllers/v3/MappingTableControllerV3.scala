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

import io.swagger.v3.oas.annotations.Parameter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.rest_api.services.v3.MappingTableServiceV3

import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest
import scala.concurrent.Future

@RestController
@RequestMapping(Array("/api-v3/mapping-tables"))
class MappingTableControllerV3 @Autowired()(mappingTableService: MappingTableServiceV3)
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
  def updateDefaults(@Parameter(hidden = true) @AuthenticationPrincipal user: UserDetails,
                     @PathVariable name: String,
                     @PathVariable version: String,
                     @RequestBody newDefaults: Array[DefaultValue],
                     request: HttpServletRequest
                    ): CompletableFuture[ResponseEntity[Validation]] = {
    withMappingTableToResponse(name, version, user, request) { existingMt =>
      mappingTableService.updateDefaults(user.getUsername, name, existingMt.version, newDefaults.toList)
    }
  }

  @PostMapping(path = Array("/{name}/{version}/defaults"))
  @ResponseStatus(HttpStatus.CREATED)
  def addDefault(@Parameter(hidden = true) @AuthenticationPrincipal user: UserDetails,
                 @PathVariable name: String,
                 @PathVariable version: String,
                 @RequestBody newDefault: DefaultValue,
                 request: HttpServletRequest
                ): CompletableFuture[ResponseEntity[Validation]] = {
    withMappingTableToResponse(name, version, user, request) { existingMt =>
      mappingTableService.addDefault(user.getUsername, name, existingMt.version, newDefault)
    }
  }

  private def withMappingTableToResponse(name: String, version: String, user: UserDetails, request: HttpServletRequest,
                                         stripLastSegments: Int = 3, suffix: String = s"/defaults")
                                        (updateExistingMtFn: MappingTable => Future[Option[(MappingTable, Validation)]]):
  Future[ResponseEntity[Validation]] = {
    for {
      existingMtOpt <- forVersionExpression(name, version)(mappingTableService.getVersion)
      existingMt = existingMtOpt.getOrElse(throw notFound())
      updatedMtAndValidationOpt <- updateExistingMtFn(existingMt)
      (updatedMt, validation) = updatedMtAndValidationOpt.getOrElse(throw notFound())
      response = createdWithNameVersionLocationBuilder(name, updatedMt.version, request,
        stripLastSegments, suffix).body(validation) // for .../defaults: stripping /{name}/{version}/defaults
    } yield response
  }

}
