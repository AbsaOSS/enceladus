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

import java.util.concurrent.CompletableFuture

import scala.concurrent.Future

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

import za.co.absa.enceladus.rest_api.models.LandingPageInformation
import za.co.absa.enceladus.rest_api.repositories.DatasetMongoRepository
import za.co.absa.enceladus.rest_api.repositories.LandingPageStatisticsMongoRepository
import za.co.absa.enceladus.rest_api.repositories.MappingTableMongoRepository
import za.co.absa.enceladus.rest_api.repositories.SchemaMongoRepository
import za.co.absa.enceladus.rest_api.services.RunService

@RestController
@RequestMapping(Array("/api/landing"))
class LandingPageController @Autowired() (datasetRepository: DatasetMongoRepository,
    mappingTableRepository: MappingTableMongoRepository,
    schemaRepository: SchemaMongoRepository,
    runsService: RunService,
    landingPageRepository: LandingPageStatisticsMongoRepository) extends BaseController {

  import scala.concurrent.ExecutionContext.Implicits.global
  import za.co.absa.enceladus.rest_api.utils.implicits._

  @GetMapping(path = Array("/info"))
  def retrieveLandingPageInfo(): CompletableFuture[LandingPageInformation] = {
    landingPageRepository.get()
  }

  def landingPageInfo(): Future[LandingPageInformation] = {
    for {
      dsCount <- datasetRepository.distinctCount()
      mtCount <- mappingTableRepository.distinctCount()
      schemaCount <- schemaRepository.distinctCount()
      runCount <- runsService.getCount()
      todaysStats <- runsService.getTodaysRunsStatistics()
    } yield LandingPageInformation(dsCount, mtCount, schemaCount, runCount, todaysStats)
  }

  // scalastyle:off magic.number
  @Scheduled(initialDelay = 1000, fixedDelay = 300000)
  @Async
  def scheduledLandingPageStatsRecalc(): CompletableFuture[_] = {
    logger.info("Running scheduled landing page statistics recalculation")
    for {
      newStats <- landingPageInfo()
      res <- landingPageRepository.updateStatistics(newStats)
    } yield res
  }
}