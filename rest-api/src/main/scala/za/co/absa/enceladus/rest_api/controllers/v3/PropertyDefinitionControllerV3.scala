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
import io.swagger.v3.oas.annotations.media.{Content, Schema => AosSchema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.{ExportableObject, Validation}
import za.co.absa.enceladus.rest_api.models.rest.DisabledPayload
import za.co.absa.enceladus.rest_api.services.v3.PropertyDefinitionServiceV3

import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest

@RestController
@RequestMapping(path = Array("/api-v3/property-definitions/datasets"), produces = Array("application/json"))
class PropertyDefinitionControllerV3 @Autowired()(propertyDefinitionService: PropertyDefinitionServiceV3)
  extends VersionedModelControllerV3(propertyDefinitionService) {

  // super-class implementation is sufficient, but the following changing endpoints need admin-auth

  @PostMapping(Array("/{name}/import"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ApiResponse(responseCode = "403", description = "Forbidden", content = Array(new Content(schema = new AosSchema())))
  override def importSingleEntity(@Parameter(hidden = true) @AuthenticationPrincipal principal: UserDetails,
                         @PathVariable name: String,
                         @RequestBody importObject: ExportableObject[PropertyDefinition],
                         request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {
    super.importSingleEntity(principal, name, importObject, request)
  }

  @PostMapping(Array(""))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ApiResponse(responseCode = "403", description = "Forbidden", content = Array(new Content(schema = new AosSchema())))
  override def create(@Parameter(hidden = true) @AuthenticationPrincipal principal: UserDetails,
             @RequestBody body: PropertyDefinition,
             request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    super.create(principal, body, request)
  }

  @PutMapping(Array("/{name}/{version}"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ApiResponse(responseCode = "403", description = "Forbidden", content = Array(new Content(schema = new AosSchema())))
  override def edit(@Parameter(hidden = true) @AuthenticationPrincipal user: UserDetails,
           @PathVariable name: String,
           @PathVariable version: Int,
           @RequestBody item: PropertyDefinition,
           request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    super.edit(user, name, version, item, request)
  }

  @DeleteMapping(Array("/{name}"))
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ApiResponse(responseCode = "403", description = "Forbidden", content = Array(new Content(schema = new AosSchema())))
  override def disable(@PathVariable name: String): CompletableFuture[DisabledPayload] = {
    super.disable(name)
  }
}

