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

import com.mongodb.client.result.UpdateResult
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.versionedModel._
import za.co.absa.enceladus.model.{ExportableObject, UsedIn}
import za.co.absa.enceladus.rest_api.controllers.BaseController
import za.co.absa.enceladus.rest_api.exceptions.NotFoundException
import za.co.absa.enceladus.rest_api.services.VersionedModelService

import java.util.Optional
import java.util.concurrent.CompletableFuture

abstract class VersionedModelControllerV3[C <: VersionedModel with Product
  with Auditable[C]](versionedModelService: VersionedModelService[C]) extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  // todo maybe offset/limit?
  @GetMapping(Array(""))
  @ResponseStatus(HttpStatus.OK)
  def getList(@RequestParam searchQuery: Optional[String]): CompletableFuture[Seq[VersionedSummary]] = {
    versionedModelService.getLatestVersionsSummary(searchQuery.toScalaOption)
  }

  @GetMapping(Array("/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def getVersionsList(@PathVariable name: String): CompletableFuture[Seq[Int]] = {
    versionedModelService.getAllVersionsValues(name)
  }

  @GetMapping(Array("/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def getVersionDetail(@PathVariable name: String,
                       @PathVariable version: Int): CompletableFuture[C] = {
    versionedModelService.getVersion(name, version).map {
      case Some(entity) => entity
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{name}/latest"))
  @ResponseStatus(HttpStatus.OK)
  def getLatestDetail(@PathVariable name: String): CompletableFuture[C] = {
    versionedModelService.getLatestVersion(name).map {
      case Some(entity) => entity
      case None         => throw NotFoundException()
    }
  }

  @GetMapping(Array("/{name}/audit-trail"))
  @ResponseStatus(HttpStatus.OK)
  def getAuditTrail(@PathVariable name: String): CompletableFuture[AuditTrail] = {
    versionedModelService.getAuditTrail(name)
  }

  @GetMapping(Array("/{name}/{version}/used-in"))
  @ResponseStatus(HttpStatus.OK)
  def usedIn(@PathVariable name: String,
      @PathVariable version: Int): CompletableFuture[UsedIn] = {
    versionedModelService.getUsedIn(name, Some(version))
  }

  @GetMapping(Array("/{name}/{version}/export"))
  @ResponseStatus(HttpStatus.OK)
  def exportSingleEntity(@PathVariable name: String, @PathVariable version: Int): CompletableFuture[String] = {
    versionedModelService.exportSingleItem(name, version)
  }

  @GetMapping(Array("/{name}/export"))
  @ResponseStatus(HttpStatus.OK)
  def exportLatestEntity(@PathVariable name: String): CompletableFuture[String] = {
    versionedModelService.exportLatestItem(name)
  }

  @PostMapping(Array("/{name}/import"))
  @ResponseStatus(HttpStatus.CREATED)
  def importSingleEntity(@AuthenticationPrincipal principal: UserDetails,
                         @PathVariable name: String,
                         @RequestBody importObject: ExportableObject[C]): CompletableFuture[C] = {
    // todo check that the name pathVar and object conform
    versionedModelService.importSingleItem(importObject.item, principal.getUsername, importObject.metadata).map {
      case Some(entity) => entity // todo redo to have header Location present
      case None         => throw notFound()
    }
  }

  @PostMapping(Array(""))
  @ResponseStatus(HttpStatus.CREATED)
  def create(@AuthenticationPrincipal principal: UserDetails, @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.isDisabled(item.name).flatMap { isDisabled =>
      if (isDisabled) {
        versionedModelService.recreate(principal.getUsername, item)
      } else {
        versionedModelService.create(item, principal.getUsername)
      }
    }.map {
      case Some(entity) => entity // todo redo to have header Location present
      case None         => throw notFound()
    }
  }

  @PutMapping(Array(""))
  @ResponseStatus(HttpStatus.OK)
  def edit(@AuthenticationPrincipal user: UserDetails,
      @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.update(user.getUsername, item).map {
      case Some(entity) => entity
      case None         => throw notFound()
    }
  }

  @DeleteMapping(Array("/{name}", "/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def disable(@PathVariable name: String,
      @PathVariable version: Optional[String]): CompletableFuture[UpdateResult] = {
    val v = if (version.isPresent) {
      // For some reason Spring reads the Optional[Int] param as a Optional[String] and then throws ClassCastException
      Some(version.get.toInt)
    } else {
      None
    }
    versionedModelService.disableVersion(name, v)
  }

}
