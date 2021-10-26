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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RestController}
import za.co.absa.enceladus.rest_api.services.StatisticsService
import za.co.absa.enceladus.model.properties.PropertyDefinitionStats

@RestController
@RequestMapping(Array("/api/statistics"))
class StatisticsController @Autowired() (statisticsService: StatisticsService) extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  @GetMapping(Array("/properties/missing"))
  def getPropertiesWithMissingCount(): CompletableFuture[Seq[PropertyDefinitionStats]] = {
    statisticsService.getPropertiesWithMissingCount()
  }

}
