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
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.servlet.support.ServletUriComponentsBuilder
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.versionedModel._
import za.co.absa.enceladus.model.{ExportableObject, UsedIn}
import za.co.absa.enceladus.rest_api.controllers.{BaseController}
import za.co.absa.enceladus.rest_api.controllers.v3.VersionedModelControllerV3.LatestVersionKey
import za.co.absa.enceladus.rest_api.services.VersionedModelService

import java.net.URI
import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import javax.servlet.http.HttpServletRequest
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

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
  def getVersionsList(@PathVariable name: String): CompletableFuture[VersionsList] = {
    versionedModelService.getAllVersionsValues(name) map {
      case Some(entity) => entity
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

  @GetMapping(Array("/{name}/{version}/export"))
  @ResponseStatus(HttpStatus.OK)
  def exportSingleEntity(@PathVariable name: String, @PathVariable version: String): CompletableFuture[String] = {
    forVersionExpression(name, version)(versionedModelService.exportSingleItem)
  }

  @GetMapping(Array("/{name}/export"))
  @ResponseStatus(HttpStatus.OK)
  def exportLatestEntity(@PathVariable name: String): CompletableFuture[String] = {
    versionedModelService.exportLatestItem(name) // todo: remove in favor of the above? (that supports /{name}/latest/export)
  }

  @PostMapping(Array("/{name}/import"))
  @ResponseStatus(HttpStatus.CREATED)
  def importSingleEntity(@AuthenticationPrincipal principal: UserDetails,
                         @PathVariable name: String,
                         @RequestBody importObject: ExportableObject[C],
                         request: HttpServletRequest): CompletableFuture[ResponseEntity[Nothing]] = {
    if (name != importObject.item.name) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity name mismatch: '$name' != '${importObject.item.name}'"))
    } else {
      versionedModelService.importSingleItem(importObject.item, principal.getUsername, importObject.metadata).map {
        case Some(entity) =>
          // stripping two last segments, instead of /api-v3/dastasets/dsName/import + /dsName/dsVersion we want /api-v3/dastasets + /dsName/dsVersion
          createdWithNameVersionLocation(entity.name, entity.version, request, stripLastSegments = 2)
        case None => throw notFound()
      }
    }
  }

  @PostMapping(Array(""))
  @ResponseStatus(HttpStatus.CREATED)
  def create(@AuthenticationPrincipal principal: UserDetails,
             @RequestBody item: C,
             request: HttpServletRequest): CompletableFuture[ResponseEntity[Nothing]] = {
    versionedModelService.isDisabled(item.name).flatMap { isDisabled =>
      if (isDisabled) {
        versionedModelService.recreate(principal.getUsername, item)
      } else {
        versionedModelService.create(item, principal.getUsername)
      }
    }.map {
      case Some(entity) => createdWithNameVersionLocation(entity.name, entity.version, request)
      case None => throw notFound()
    }
  }

  @PutMapping(Array("/{name}/{version}"))
  @ResponseStatus(HttpStatus.NO_CONTENT)
  def edit(@AuthenticationPrincipal user: UserDetails,
           @PathVariable name: String,
           @PathVariable version: Int,
           @RequestBody item: C,
           request: HttpServletRequest): CompletableFuture[ResponseEntity[Nothing]] = {

    if (name != item.name) {
      Future.failed(new IllegalArgumentException(s"URL and payload entity name mismatch: '$name' != '${item.name}'"))
    } else if (version != item.version) {
      Future.failed(new IllegalArgumentException(s"URL and payload version mismatch: ${version} != ${item.version}"))
    } else {
      versionedModelService.update(user.getUsername, item).map {
        case Some(entity) => createdWithNameVersionLocation(entity.name, entity.version, request, stripLastSegments = 2)
        case None => throw notFound()
      }
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

  protected def createdWithNameVersionLocation(name: String, version: Int, request: HttpServletRequest,
                                               stripLastSegments: Int = 0, suffix: String = ""): ResponseEntity[Nothing] = {
    val strippingPrefix = Range(0, stripLastSegments).map(_ => "/..").mkString

    val location: URI = ServletUriComponentsBuilder.fromRequest(request)
      .path(s"$strippingPrefix/{name}/{version}$suffix")
      .buildAndExpand(name, version.toString)
      .normalize() // will normalize `/one/two/../three` into `/one/tree`
      .toUri() // will create location e.g. http:/domain.ext/api-v3/dataset/MyExampleDataset/1

    // hint on "/.." + normalize https://github.com/spring-projects/spring-framework/issues/14905#issuecomment-453400918

    ResponseEntity.created(location).build()
  }

}

object VersionedModelControllerV3 {
  val LatestVersionKey = "latest"
}
