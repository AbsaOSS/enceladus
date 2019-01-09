/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.rest.controllers

import java.util.Optional
import java.util.concurrent.CompletableFuture

import com.mongodb.client.result.UpdateResult
import org.slf4j.LoggerFactory
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation.{GetMapping, PathVariable, PostMapping, RequestBody}
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.versionedModel._
import za.co.absa.enceladus.rest.services.VersionedModelService


abstract class VersionedModelController[C <: VersionedModel](versionedModelService: VersionedModelService[C]) {

  import za.co.absa.enceladus.rest.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global
  private[controllers] val logger = LoggerFactory.getLogger(this.getClass)

  @GetMapping(path = Array("/list"))
  def getList(): CompletableFuture[Seq[VersionedSummary]] = {
    versionedModelService.getLatestVersions()
  }

  @GetMapping(path = Array("/detail/{name}/{version}"))
  def getVersionDetail(@PathVariable name: String, @PathVariable version: Int): CompletableFuture[C] = {
    versionedModelService.getVersion(name, version)
  }

  @GetMapping(path = Array("/detail/{name}/latest"))
  def getLatestDetail(@PathVariable name: String): CompletableFuture[C] = {
    versionedModelService.getLatestVersion(name)
  }

  @GetMapping(path = Array("/isUniqueName/{name}"))
  def isUniqueName(@PathVariable name: String): CompletableFuture[Boolean] = {
    versionedModelService.isUniqueName(name)
  }

  @GetMapping(path = Array("/usedIn/{name}/{version}"))
  def usedIn(@PathVariable name: String, @PathVariable version: Int): CompletableFuture[UsedIn] = {
    versionedModelService.getUsedIn(name, Some(version))
  }

  @GetMapping(path = Array("/allVersions/{name}"))
  def getAllVersions(@PathVariable name: String): CompletableFuture[Seq[C]] = {
    versionedModelService.getAllVersions(name)
  }

  @PostMapping(path = Array("/create"))
  def create(@AuthenticationPrincipal principal: UserDetails, @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.create(item, principal.getUsername)
  }

  @PostMapping(path = Array("/edit"))
  def edit(@AuthenticationPrincipal user: UserDetails, @RequestBody item: C): CompletableFuture[C] = {
    versionedModelService.update(user.getUsername, item)
  }

  @GetMapping(path = Array("/disable/{name}", "/disable/{name}/{version}"))
  def disable(@PathVariable name: String, @PathVariable version: Optional[Int]): CompletableFuture[Object] = {
    val v = if (version.isPresent) Some(version.get) else None
    val res = versionedModelService.disableVersion(name, v)

    for {
      r <- res
    } yield r match {
      case l: UsedIn       => l.toSeq
      case u: UpdateResult => u
    }
  }

}
