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

package za.co.absa.enceladus.rest_api.controllers.v3

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.rest_api.services.DatasetService

@RestController
@RequestMapping(path = Array("/api-v3/datasets"))
class DatasetControllerV3 @Autowired()(datasetService: DatasetService)
  extends VersionedModelControllerV3(datasetService) {

  // TODO
  // /{datasetName}/{version}/rules
  // /{datasetName}/{version}/rules/{index}
  // /{datasetName}/{version}/rules

}


