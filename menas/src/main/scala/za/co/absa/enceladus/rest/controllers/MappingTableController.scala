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

import java.util.concurrent.CompletableFuture

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.rest.services.MappingTableService

@RestController
@RequestMapping(Array("/api/mappingTable"))
class MappingTableController @Autowired() (mappingTableService: MappingTableService)
  extends VersionedModelController(mappingTableService) {

  import za.co.absa.enceladus.rest.utils.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(path = Array("/updateDefaults"))
  @ResponseStatus(HttpStatus.OK)
  def updateDefaults(@AuthenticationPrincipal user: UserDetails,
                     @RequestBody upd: MenasObject[Array[DefaultValue]]): CompletableFuture[MappingTable] = {
    mappingTableService.update(user.getUsername, upd.id.name) { mt =>
      mt.setDefaultMappingValue(upd.value.toList)
    }.map {
      case Some(entity) => entity
      case None         => throw notFound()
    }
  }

  @PostMapping(path = Array("/addDefault"))
  @ResponseStatus(HttpStatus.OK)
  def addDefault(@AuthenticationPrincipal user: UserDetails,
                 @RequestBody newDefault: MenasObject[DefaultValue]): CompletableFuture[MappingTable] = {
    mappingTableService.update(user.getUsername, newDefault.id.name) { mt =>
      mt.setDefaultMappingValue(mt.defaultMappingValue :+ newDefault.value)
    }.map {
      case Some(entity) => entity
      case None         => throw notFound()
    }
  }

}
