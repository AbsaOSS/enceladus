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

import scala.concurrent.Future
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import za.co.absa.enceladus.menas.models.LandingPageInformation
import za.co.absa.enceladus.menas.repositories.DatasetMongoRepository
import za.co.absa.enceladus.menas.repositories.LandingPageStatisticsMongoRepository
import za.co.absa.enceladus.menas.repositories.MappingTableMongoRepository
import za.co.absa.enceladus.menas.repositories.SchemaMongoRepository
import za.co.absa.enceladus.menas.services.{PropertyDefinitionService, RunService, StatisticsService}
import za.co.absa.enceladus.model.properties.essentiality.{Mandatory, Recommended}

@RestController
@RequestMapping(Array("/api/landing"))
class LandingPageController @Autowired() (datasetRepository: DatasetMongoRepository,
    mappingTableRepository: MappingTableMongoRepository,
    schemaRepository: SchemaMongoRepository,
    runsService: RunService,
    landingPageRepository: LandingPageStatisticsMongoRepository,
    statisticsService: StatisticsService) extends BaseController {

  import scala.concurrent.ExecutionContext.Implicits.global
  import za.co.absa.enceladus.menas.utils.implicits._

  @GetMapping(path = Array("/info"))
  def retrieveLandingPageInfo(): CompletableFuture[LandingPageInformation] = {
    landingPageRepository.get()
  }

  def landingPageInfo(): Future[LandingPageInformation] = {
    val dsCountFuture = datasetRepository.distinctCount()
    val mappingTableFuture = mappingTableRepository.distinctCount()
    val schemaFuture = schemaRepository.distinctCount()
    val runFuture = runsService.getCount()
    val propertiesWithMissingCountsFuture = statisticsService.getPropertiesWithMissingCount()
    val propertiesTotalsFuture: Future[(Int, Int, Int)] = propertiesWithMissingCountsFuture.map(props => {
      props.foldLeft(0, 0, 0) { (acum, item) =>
        val (count, mandatoryCount, recommendedCount) = acum
        item.essentiality match {
          case Mandatory(_) => (count + 1, mandatoryCount + item.missingInDatasetsCount, recommendedCount)
          case Recommended() => (count + 1, mandatoryCount, recommendedCount + item.missingInDatasetsCount)
          case _ => (count + 1, mandatoryCount, recommendedCount)
        }
      }
    })
    val todaysStatsfuture = runsService.getTodaysRunsStatistics()
    for {
      dsCount <- dsCountFuture
      mtCount <- mappingTableFuture
      schemaCount <- schemaFuture
      runCount <- runFuture
      (propertiesCount, totalMissingMandatoryProperties, totalMissingRecommendedProperties) <- propertiesTotalsFuture
      todaysStats <- todaysStatsfuture
    } yield LandingPageInformation(dsCount, mtCount, schemaCount, runCount, propertiesCount,
      totalMissingMandatoryProperties, totalMissingRecommendedProperties, todaysStats)
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
