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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RestController}
import za.co.absa.enceladus.menas.services.StatisticsService
import za.co.absa.enceladus.model.properties.PropertyDefinitionDto

import scala.concurrent.Future

@RestController
@RequestMapping(Array("/api/statistics"))
class StatisticsController @Autowired() (statisticsService: StatisticsService){

  @GetMapping(Array("/properties/missing"))
  def getPropertiesWithMissingCount(): Future[Seq[PropertyDefinitionDto]] = {
    statisticsService.getPropertiesWithMissingCount()
  }

}
