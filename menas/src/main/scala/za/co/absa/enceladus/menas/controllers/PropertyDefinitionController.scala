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
import java.util.Optional
import java.util.concurrent.CompletableFuture

import com.mongodb.client.result.UpdateResult
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.menas.services.PropertyDefinitionService
import za.co.absa.enceladus.model.ExportableObject
import za.co.absa.enceladus.model.properties.PropertyDefinition

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * This API mapping is directly connected to Dataset Properties.
 * Should we need to serve other entity properties in the future, API support may be added.
 */
@RestController
@RequestMapping(path = Array("/api/properties/datasets"), produces = Array("application/json"))
class PropertyDefinitionController @Autowired()(propertyDefService: PropertyDefinitionService)
  extends VersionedModelController(propertyDefService) {

  import za.co.absa.enceladus.menas.utils.implicits._

  @GetMapping(Array(""))
  def getAllDatasetProperties(): CompletableFuture[Seq[PropertyDefinition]] = {
    logger.info("retrieving all dataset properties in full")
    propertyDefService.getLatestVersions()
  }

  @PostMapping(Array(""))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("'menas_admin' == authentication.name")
  def createDatasetProperty(@AuthenticationPrincipal principal: UserDetails,
                            @RequestBody item: PropertyDefinition): CompletableFuture[ResponseEntity[PropertyDefinition]] = {
    // basically an alias for /create with Location header response
    logger.info(s"creating new property definition '${item.name}'")

    import scala.compat.java8.FutureConverters.CompletionStageOps // implicit wrapper with toScala for CompletableFuture
    super.create(principal, item).toScala.map{ entity =>
      val location: URI = new URI(s"/api/properties/datasets/${entity.name}/${entity.version}")
      ResponseEntity.created(location).body(entity)
    }

    // TODO: Location header would make sense for the underlying VersionedModelController.create, too. Issue #1611
  }

  @GetMapping(Array("/{propertyName}"))
  def getDatasetProperty(@PathVariable propertyName: String): CompletableFuture[PropertyDefinition] = {
    logger.info(s"retrieving property definition '$propertyName' (latest version) in full")
    // basically an alias for /detail/{name}/latest
    super.getLatestDetail(propertyName)
  }

  @GetMapping(Array("/{propertyName}/{version}"))
  def getDatasetProperty(@PathVariable propertyName: String, @PathVariable version: Int): CompletableFuture[PropertyDefinition] = {
    logger.info(s"retrieving property definition '$propertyName' (version $version) in full")
    // basically an alias for /detail/{name}/{version}
    super.getVersionDetail(propertyName, version)
  }

  @PostMapping(Array("/importItem"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("'menas_admin' == authentication.name")
  override def importSingleEntity(@AuthenticationPrincipal principal: UserDetails,
                                  @RequestBody importObject: ExportableObject[PropertyDefinition]): CompletableFuture[PropertyDefinition] =
    super.importSingleEntity(principal, importObject)

  @RequestMapping(method = Array(RequestMethod.POST, RequestMethod.PUT), path = Array("/edit"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("'menas_admin' == authentication.name")
  override def edit(@AuthenticationPrincipal user: UserDetails,
                    @RequestBody item: PropertyDefinition): CompletableFuture[PropertyDefinition] =
    super.edit(user, item)

  @DeleteMapping(Array("/disable/{name}", "/disable/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("'menas_admin' == authentication.name")
  override def disable(@PathVariable name: String,
                       @PathVariable version: Optional[String]): CompletableFuture[UpdateResult] =
    super.disable(name, version)

  @PostMapping(Array("/create"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("'menas_admin' == authentication.name")
  override def create(@AuthenticationPrincipal principal: UserDetails,
                      @RequestBody item: PropertyDefinition): CompletableFuture[PropertyDefinition] =
    super.create(principal, item)
}
