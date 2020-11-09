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

import java.util.concurrent.CompletableFuture

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.menas.services.PropertyDefinitionService
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.propertyType.{StringEnumPropertyType, StringPropertyType}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Random

@RestController
@RequestMapping(path = Array("/api/properties"), produces = Array("application/json"))
class PropertyDefinitionController @Autowired()(propertyDefService: PropertyDefinitionService)
  extends VersionedModelController(propertyDefService) {

  import za.co.absa.enceladus.menas.utils.implicits._


  @GetMapping(Array("/datasets"))
  def getAllDatasetProperties(): CompletableFuture[java.util.List[PropertyDefinition]] = {
    // todo implement
    logger.info("retrieving all dataset properties")
    Future.successful(List.empty[PropertyDefinition].asJava)
  }

  @GetMapping(Array("/datasets/{propertyName}"))
  def getDatasetProperty(@PathVariable propertyName: String): CompletableFuture[Option[PropertyDefinition]] = {
    // todo implement
    // todo particular version, too
    logger.info(s"retrieving dataset properties by name $propertyName")
    propertyDefService.getLatestVersion(propertyName) // 404 when not found
  }

  @PostMapping(Array("/datasets/testcreate")) // todo remove/use as inspiration for a integTest?
  @ResponseStatus(HttpStatus.CREATED)
  def addConformanceRule(@AuthenticationPrincipal user: UserDetails): CompletableFuture[Seq[PropertyDefinition]] = {

    val testProperty = PropertyDefinition(
      name = s"testProp${Random.nextLong().abs}",
      propertyType = StringPropertyType()
    )
    val prop1 = propertyDefService.create(testProperty, user.getUsername)

    val testProperty2 = PropertyDefinition(
      name = s"testProp${Random.nextLong().abs}",
      propertyType = StringEnumPropertyType(Set("optionA", "optionB", "optionC")),
      suggestedValue = "optionB"
    )

    import scala.concurrent.ExecutionContext.Implicits.global
    val prop2 = propertyDefService.create(testProperty2, user.getUsername)

    for {
      res1 <- prop1.map(_.toSeq)
      res2 <- prop2.map(_.toSeq)
    } yield (res1 ++ res2)

  }

}
