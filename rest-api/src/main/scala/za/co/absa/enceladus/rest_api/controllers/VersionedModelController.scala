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

package za.co.absa.enceladus.rest_api.controllers

import java.util.Optional
import java.util.concurrent.CompletableFuture
import com.mongodb.client.result.UpdateResult
import org.springframework.http.HttpStatus
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.{ExportableObject, UsedIn}
import za.co.absa.enceladus.model.versionedModel.{VersionedModel, VersionedSummaryV2}
import za.co.absa.enceladus.rest_api.exceptions.NotFoundException
import za.co.absa.enceladus.rest_api.services.VersionedModelService
import za.co.absa.enceladus.model.backend.audit._


abstract class VersionedModelController[C <: VersionedModel with Product with Auditable[C]](versionedModelService: VersionedModelService[C])
  extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @GetMapping(Array("/list", "/list/{searchQuery}"))
  @ResponseStatus(HttpStatus.OK)
  def getList(@PathVariable searchQuery: Optional[String]): CompletableFuture[Seq[VersionedSummaryV2]] = {
    versionedModelService.getLatestVersionsSummarySearch(searchQuery.toScalaOption, None, None) // V2 knows no skip/limit
      .map(_.map(_.toV2))
  }

  @GetMapping(Array("/searchSuggestions"))
  @ResponseStatus(HttpStatus.OK)
  def getSearchSuggestions(): CompletableFuture[Seq[String]] = {
    versionedModelService.getSearchSuggestions()
  }

  @GetMapping(Array("/detail/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def getVersionDetail(@PathVariable name: String,
      @PathVariable version: Int): CompletableFuture[C] = {
    versionedModelService.getVersion(name, version).map {
      case Some(entity) => entity
      case None         => throw notFound()
    }
  }

  @GetMapping(Array("/detail/{name}/latest"))
  @ResponseStatus(HttpStatus.OK)
  def getLatestDetail(@PathVariable name: String): CompletableFuture[C] = {
    versionedModelService.getLatestVersion(name).map {
      case Some(entity) => entity
      case None         => throw NotFoundException()
    }
  }

  @GetMapping(Array("/detail/{name}/latestVersion"))
  @ResponseStatus(HttpStatus.OK)
  def getLatestVersionNumber(@PathVariable name: String): CompletableFuture[Int] = {
    versionedModelService.getLatestVersionNumber(name)
  }

  @GetMapping(Array("/detail/{name}/audit"))
  @ResponseStatus(HttpStatus.OK)
  def getAuditTrail(@PathVariable name: String): CompletableFuture[AuditTrail] = {
    versionedModelService.getAuditTrail(name)
  }

  @GetMapping(Array("/isUniqueName/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def isUniqueName(@PathVariable name: String): CompletableFuture[Boolean] = {
    versionedModelService.isUniqueName(name)
  }

  @GetMapping(Array("/usedIn/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def usedIn(@PathVariable name: String,
      @PathVariable version: Int): CompletableFuture[UsedIn] = {
    versionedModelService.getUsedIn(name, Some(version))
  }

  @GetMapping(Array("/allVersions/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def getAllVersions(@PathVariable name: String): CompletableFuture[Seq[C]] = {
    versionedModelService.getAllVersions(name)
  }

  @GetMapping(Array("/exportItem/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def exportSingleEntity(@PathVariable name: String, @PathVariable version: Int): CompletableFuture[String] = {
    versionedModelService.exportSingleItem(name, version)
  }

  @GetMapping(Array("/exportItem/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def exportLatestEntity(@PathVariable name: String): CompletableFuture[String] = {
    versionedModelService.exportLatestItem(name)
  }

  @PostMapping(Array("/importItem"))
  @ResponseStatus(HttpStatus.CREATED)
  def importSingleEntity(@AuthenticationPrincipal principal: UserDetails,
                         @RequestBody importObject: ExportableObject[C]): CompletableFuture[C] = {
    versionedModelService.importSingleItem(importObject.item, principal.getUsername, importObject.metadata).map {
      case Some((entity, validation)) => entity // validation is disregarded for V2, import-v2 has its own
      case None         => throw notFound()
    }
  }

  @PostMapping(Array("/create"))
  @ResponseStatus(HttpStatus.CREATED)
  def create(@AuthenticationPrincipal principal: UserDetails, @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.isDisabled(item.name).flatMap { isDisabled =>
      if (isDisabled) {
        versionedModelService.recreate(principal.getUsername, item)
      } else {
        versionedModelService.create(item, principal.getUsername)
      }
    }.map {
      case Some((entity, validation)) => entity // v2 does not support validation-warnings on create
      case None         => throw notFound()
    }
  }

  @RequestMapping(method = Array(RequestMethod.POST, RequestMethod.PUT), path = Array("/edit"))
  @ResponseStatus(HttpStatus.CREATED)
  def edit(@AuthenticationPrincipal user: UserDetails,
      @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.update(user.getUsername, item).map {
      case Some((entity, validation)) => entity // v2 disregarding validation on edit
      case None         => throw notFound()
    }
  }

  @DeleteMapping(Array("/disable/{name}", "/disable/{name}/{version}"))
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

  @PutMapping(Array("/lock/{name}"))
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ResponseStatus(HttpStatus.OK)
  def lock(@AuthenticationPrincipal principal: UserDetails,
           @PathVariable name: String): CompletableFuture[UpdateResult] = {
    versionedModelService.setLock(name, isLocked = true, principal)
  }

  @PutMapping(Array("/unlock/{name}"))
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  @ResponseStatus(HttpStatus.OK)
  def unlock(@AuthenticationPrincipal principal: UserDetails,
             @PathVariable name: String): CompletableFuture[UpdateResult] = {
    versionedModelService.setLock(name, isLocked = false, principal)
  }


}
