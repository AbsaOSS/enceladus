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

import io.swagger.v3.oas.annotations.media.{Content, Schema => AosSchema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.rest_api.services.MonitoringService

import java.util.concurrent.CompletableFuture

@RestController
@RequestMapping(Array("/api/monitoring"))
@SecurityRequirement(name = "JWT")
@ApiResponse(responseCode = "401", description = "Unauthorized", content = Array(new Content(schema = new AosSchema())))
class MonitoringController @Autowired()(monitoringService: MonitoringService)
  extends BaseController {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  @GetMapping(value = Array("data/datasets/{datasetName}/{startDate}/{endDate}"), produces = Array("application/json"))
  @ResponseStatus(HttpStatus.OK)
  def getMonitoringDataPoints(@PathVariable datasetName: String,
                              @PathVariable startDate: String,
                              @PathVariable endDate: String): CompletableFuture[String] = {
    monitoringService.getMonitoringDataPoints(datasetName, startDate, endDate)
}

}
