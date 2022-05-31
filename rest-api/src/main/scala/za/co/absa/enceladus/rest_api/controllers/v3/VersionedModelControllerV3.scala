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

import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.servlet.support.ServletUriComponentsBuilder
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.versionedModel._
import za.co.absa.enceladus.model.{ExportableObject, UsedIn, Validation}
import za.co.absa.enceladus.rest_api.controllers.BaseController
import za.co.absa.enceladus.rest_api.controllers.v3.VersionedModelControllerV3.LatestVersionKey
import za.co.absa.enceladus.rest_api.exceptions.NotFoundException
import za.co.absa.enceladus.rest_api.models.rest.DisabledPayload
import za.co.absa.enceladus.rest_api.services.v3.VersionedModelServiceV3

import java.net.URI
import java.util.Optional
import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object VersionedModelControllerV3 {
  final val LatestVersionKey = "latest"
}

abstract class VersionedModelControllerV3[C <: VersionedModel with Product
  with Auditable[C]](versionedModelService: VersionedModelServiceV3[C]) extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  // todo maybe offset/limit -> Issue #2060
  @GetMapping(Array(""))
  @ResponseStatus(HttpStatus.OK)
  def getList(@RequestParam searchQuery: Optional[String]): CompletableFuture[Seq[NamedVersion]] = {
    versionedModelService.getLatestVersionsSummarySearch(searchQuery.toScalaOption)
      .map(_.map(_.toNamedVersion))
  }

  @GetMapping(Array("/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def getVersionSummaryForEntity(@PathVariable name: String): CompletableFuture[NamedVersion] = {
    versionedModelService.getLatestVersionSummary(name).map {
      case Some(entity) => entity.toNamedVersion
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def getVersionDetail(@PathVariable name: String,
                       @PathVariable version: String): CompletableFuture[C] = {
    forVersionExpression(name, version)(versionedModelService.getVersion).map {
      case Some(entity) => entity
      case None => throw notFound()
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
             @PathVariable version: String): CompletableFuture[UsedIn] = {
    forVersionExpression(name, version) { case (name, versionInt) => versionedModelService.getUsedIn(name, Some(versionInt)) }
  }

  @GetMapping(Array("/{name}/used-in"))
  @ResponseStatus(HttpStatus.OK)
  def usedIn(@PathVariable name: String): CompletableFuture[UsedIn] = {
    versionedModelService.getUsedIn(name, None)
  }

  @GetMapping(Array("/{name}/{version}/export"))
  @ResponseStatus(HttpStatus.OK)
  def exportSingleEntity(@PathVariable name: String, @PathVariable version: String): CompletableFuture[String] = {
    forVersionExpression(name, version)(versionedModelService.exportSingleItem)
  }

  @PostMapping(Array("/{name}/import"))
  @ResponseStatus(HttpStatus.CREATED)
  def importSingleEntity(@AuthenticationPrincipal principal: UserDetails,
                         @PathVariable name: String,
                         @RequestBody importObject: ExportableObject[C],
                         request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {
    if (name != importObject.item.name) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity name mismatch: '$name' != '${importObject.item.name}'"))
    } else {
      versionedModelService.importSingleItem(importObject.item, principal.getUsername, importObject.metadata).map {
        case Some((entity, validation)) =>
          // stripping two last segments, instead of /api-v3/dastasets/dsName/import + /dsName/dsVersion we want /api-v3/dastasets + /dsName/dsVersion
          createdWithNameVersionLocationBuilder(entity.name, entity.version, request, stripLastSegments = 2).body(validation)
        case None => throw notFound()
      }
    }
  }

  @GetMapping(Array("/{name}/{version}/validation"))
  @ResponseStatus(HttpStatus.OK)
  def validation(@PathVariable name: String,
                 @PathVariable version: String): CompletableFuture[Validation] = {
    forVersionExpression(name, version)(versionedModelService.validate)
  }

  @PostMapping(Array(""))
  @ResponseStatus(HttpStatus.CREATED)
  def create(@AuthenticationPrincipal principal: UserDetails,
             @RequestBody item: C,
             request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    // enabled check is part of the validation
        versionedModelService.create(item, principal.getUsername)
      .map {
      case Some((entity, validation)) => createdWithNameVersionLocationBuilder(entity.name, entity.version, request).body(validation)
      case None => throw notFound()
    }
  }

  @PutMapping(Array("/{name}/{version}"))
  @ResponseStatus(HttpStatus.CREATED)
  def edit(@AuthenticationPrincipal user: UserDetails,
           @PathVariable name: String,
           @PathVariable version: Int,
           @RequestBody item: C,
           request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    if (name != item.name) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity name mismatch: '$name' != '${item.name}'"))
    } else if (version != item.version) {
      Future.failed(new IllegalArgumentException(s"URL and payload version mismatch: ${version} != ${item.version}"))
    } else {
      // disable check is already part of V3 validation
      versionedModelService.update(user.getUsername, item).map {
        case Some((updatedEntity, validation)) =>
          createdWithNameVersionLocationBuilder(updatedEntity.name, updatedEntity.version, request, stripLastSegments = 2).body(validation)
        case None => throw notFound()
      }
    }
  }

  @PutMapping(Array("/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def enable(@PathVariable name: String): CompletableFuture[DisabledPayload] = {
    versionedModelService.enableEntity(name).map { updateResult => // always enabling all version of the entity
      if (updateResult.getMatchedCount > 0) {
        DisabledPayload(disabled = false)
      } else {
        throw NotFoundException(s"No versions for entity $name found to be enabled.")
      }
    }
  }

  @DeleteMapping(Array("/{name}"))
  @ResponseStatus(HttpStatus.OK)
  def disable(@PathVariable name: String): CompletableFuture[DisabledPayload] = {
    versionedModelService.disableVersion(name, None).map { updateResult => // always disabling all version of the entity
      if (updateResult.getMatchedCount > 0) {
        DisabledPayload(disabled = true)
      } else {
        throw NotFoundException(s"No versions for entity $name found to be disabled.")
      }
    }
  }

  /**
   * For entity's name and version expression (either a number or 'latest'), the forVersionFn is called.
   *
   * @param name
   * @param versionStr
   * @param forVersionFn
   * @return
   */
  protected def forVersionExpression[T](name: String, versionStr: String)
                                       (forVersionFn: (String, Int) => Future[T]): Future[T] = {
    versionStr.toLowerCase match {
      case LatestVersionKey =>
        versionedModelService.getLatestVersionValue(name).flatMap {
          case None => Future.failed(notFound())
          case Some(actualLatestVersion) => forVersionFn(name, actualLatestVersion)
        }
      case nonLatestVersionString => Try(nonLatestVersionString.toInt) match {
        case Success(actualVersion) => forVersionFn(name, actualVersion)
        case Failure(exception) =>
          Future.failed(new IllegalArgumentException(s"Cannot convert '$versionStr' to a valid version expression. " +
            s"Either use 'latest' or an actual version number. Underlying problem: ${exception.getMessage}"))
      }
    }
  }

  protected def createdWithNameVersionLocationBuilder(name: String, version: Int, request: HttpServletRequest,
                                                      stripLastSegments: Int = 0, suffix: String = ""): ResponseEntity.BodyBuilder = {
    val strippingPrefix = Range(0, stripLastSegments).map(_ => "/..").mkString

    val location: URI = ServletUriComponentsBuilder.fromRequest(request)
      .path(s"$strippingPrefix/{name}/{version}$suffix")
      .buildAndExpand(name, version.toString)
      .normalize // will normalize `/one/two/../three` into `/one/tree`
      .toUri // will create location e.g. http:/domain.ext/api-v3/dataset/MyExampleDataset/1

    // hint on "/.." + normalize https://github.com/spring-projects/spring-framework/issues/14905#issuecomment-453400918

    ResponseEntity.created(location)
  }

}
