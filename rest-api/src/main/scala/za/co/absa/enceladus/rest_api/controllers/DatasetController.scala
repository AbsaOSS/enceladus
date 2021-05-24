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

import java.net.URI
import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.rest_api.services.DatasetService
import za.co.absa.enceladus.utils.validation.ValidationLevel.{NoValidationName, ValidationLevel}
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.{Dataset, Validation}

import scala.concurrent.Future
import scala.util.Try

@RestController
@RequestMapping(path = Array("/api/dataset"))
class DatasetController @Autowired()(datasetService: DatasetService)
  extends VersionedModelController(datasetService) {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(Array("/{datasetName}/rule/create"))
  @ResponseStatus(HttpStatus.OK)
  def addConformanceRule(@AuthenticationPrincipal user: UserDetails,
                         @PathVariable datasetName: String,
                         @RequestBody rule: ConformanceRule): CompletableFuture[Dataset] = {
    //TODO: we need to figure out how to deal with versioning properly from UX perspective
    for {
      latestVersion <- datasetService.getLatestVersionValue(datasetName)
      res <- latestVersion match {
        case Some(version) => datasetService.addConformanceRule(user.getUsername, datasetName, version, rule).map {
          case Some(ds) => ds
          case _        => throw notFound()
        }
        case _ => throw notFound()
      }
    } yield res
  }


  @GetMapping(Array("/{datasetName}/{datasetVersion}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def getAllPropertiesForVersion(@PathVariable("datasetName") datasetName: String,
                                 @PathVariable("datasetVersion") datasetVersion: Int,
                                 @RequestParam allFilterParams: util.Map[String, String] // special case: all params into a java.util.Map
                                ): CompletableFuture[Map[String, String]] = {
    getPropertiesForVersionCommon(datasetName, datasetVersion, allFilterParams)
  }

  @GetMapping(Array("/{datasetName}/properties"))
  @ResponseStatus(HttpStatus.OK)
  def getAllProperties(@PathVariable datasetName: String,
                       @RequestParam allFilterParams: util.Map[String, String]): CompletableFuture[Map[String, String]] = {
    datasetService.getLatestVersionNumber(datasetName).flatMap { version =>
      getPropertiesForVersionCommon(datasetName, version, allFilterParams)
    }
  }

  private def getPropertiesForVersionCommon(datasetName: String,
                                            datasetVersion: Int,
                                            allFilterParams: util.Map[String, String]): Future[Map[String, String]] = {
    datasetService.getVersion(datasetName, datasetVersion).flatMap {
      case Some(entity) =>
        val dsProperties = entity.propertiesAsMap

        val scalaFilterMap: Map[String, String] = allFilterParams.toScalaMap // in Scala 2.12, we could just do .asScala.toMap
        if (scalaFilterMap.isEmpty) {
          Future.successful(dsProperties) // no filtering, may even contain properties without existing definition
        } else {
          datasetService.filterProperties(dsProperties, DatasetController.paramsToPropertyDefinitionFilter(scalaFilterMap))
        }
      case None => throw notFound()
    }
  }

  @PutMapping(Array("/{datasetName}/properties"))
  @ResponseStatus(HttpStatus.CREATED)
  def replaceProperties(@AuthenticationPrincipal principal: UserDetails,
                        @PathVariable datasetName: String,
                        @RequestBody newProperties: Optional[Map[String, String]]): CompletableFuture[ResponseEntity[Option[Dataset]]] = {
    datasetService.replaceProperties(principal.getUsername, datasetName, newProperties.toScalaOption).map {
      case None => throw notFound()
      case Some(dataset) =>
        val location: URI = new URI(s"/api/dataset/${dataset.name}/${dataset.version}")
        ResponseEntity.created(location).body(Option(dataset))
    }
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/properties/valid"))
  @ResponseStatus(HttpStatus.OK)
  def getPropertiesValidation(@PathVariable datasetName: String, @PathVariable datasetVersion: Int): CompletableFuture[Validation] = {
    datasetService.getVersion(datasetName, datasetVersion).flatMap {
      case Some(entity) => datasetService.validateProperties(entity.propertiesAsMap, forRun = false)
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}/properties/validForRun"))
  @ResponseStatus(HttpStatus.OK)
  def getPropertiesValidationForRun(@PathVariable datasetName: String, @PathVariable datasetVersion: Int): CompletableFuture[Validation] = {
    datasetService.getVersion(datasetName, datasetVersion).flatMap {
      case Some(entity) => datasetService.validateProperties(entity.propertiesAsMap, forRun = true)
      case None => throw notFound()
    }
  }

  @GetMapping(Array("/{datasetName}/{datasetVersion}"))
  @ResponseStatus(HttpStatus.OK)
  def getDatasetWithPropertiesValidation(@PathVariable("datasetName") datasetName: String,
                                         @PathVariable("datasetVersion") datasetVersion: Int,
                                         @RequestParam(value = "validateProperties",
                                                       required = false,
                                                       defaultValue = NoValidationName) validate: ValidationLevel
                                        ): CompletableFuture[Dataset] = {
    datasetService.getVersionValidated(datasetName, datasetVersion, validate).map {
      case Some(ds) => ds
      case None => throw notFound()
    }
  }
}

object DatasetController {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  private[controllers] def paramsToPropertyDefinitionFilter(filterParams: Map[String, String]): PropertyDefinition => Boolean = {
    val filters: Seq[PropertyDefinition => Boolean] = filterParams
      .map { case (paramName, paramValue) => paramToPropertyDefinitionFilter(paramName, paramValue) }.toSeq

    def combineFilters(filter1: PropertyDefinition => Boolean, filter2: PropertyDefinition => Boolean): PropertyDefinition => Boolean = {
      propDef: PropertyDefinition => filter1(propDef) && filter2(propDef) // "and"-combination of filters
    }

    filters.foldLeft[PropertyDefinition => Boolean](_ => true)(combineFilters)
  }

  private[controllers] def paramToPropertyDefinitionFilter(paramName: String, paramValue: String): PropertyDefinition => Boolean = {
    propDef: PropertyDefinition =>
      paramName match {
        case "put_into_info_file" | "putIntoInfoFile" =>
          Try(paramValue.toBoolean).toOption.fold(true) {
            propDef.putIntoInfoFile == _
          } // invalid value => not filtered

        // add more cases if you want to filter using different means
        case _ =>
          log.warn(s"Invalid param $paramName=$paramValue was passed to filter properties by, ignoring it.")
          true
      }
  }
}
