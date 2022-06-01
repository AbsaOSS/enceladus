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

import org.springframework.web.bind.annotation._
import org.springframework.beans.factory.annotation.Autowired
import za.co.absa.enceladus.rest_api.services.OozieService
import java.util.concurrent.CompletableFuture
import org.springframework.http.HttpStatus
import za.co.absa.enceladus.rest_api.models.OozieCoordinatorStatus
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieSchedule
import java.util.Optional

@RestController
@RequestMapping(Array("/api/oozie"))
class OozieController @Autowired() (oozieService: OozieService) extends BaseController {
  import za.co.absa.enceladus.rest_api.utils.implicits._

  @GetMapping(path = Array("/isEnabled"))
  @ResponseStatus(HttpStatus.OK)
  def isOozieEnabled: Boolean = oozieService.isOozieEnabled

  @GetMapping(path = Array("/coordinatorStatus/{id}"))
  def getCoordinatorStatus(@PathVariable id: String): CompletableFuture[OozieCoordinatorStatus] = oozieService.getCoordinatorStatus(id)

  @PostMapping(path = Array("/runNow"))
  def runNow(@RequestBody schedule: OozieSchedule, @RequestParam reportDate: Optional[String]): CompletableFuture[String] = {
    oozieService.runNow(schedule, reportDate.toScalaOption)
  }

  @PostMapping(path = Array("/suspend/{coordId}"))
  @ResponseStatus(HttpStatus.OK)
  def suspend(@PathVariable coordId: String): CompletableFuture[OozieCoordinatorStatus] = oozieService.suspend(coordId)

  @PostMapping(path = Array("/resume/{coordId}"))
  @ResponseStatus(HttpStatus.OK)
  def resume(@PathVariable coordId: String): CompletableFuture[OozieCoordinatorStatus] = oozieService.resume(coordId)

}
