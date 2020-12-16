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

package za.co.absa.enceladus.menas.web.controllers

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.menas.web.LineageConfig

@RestController
@RequestMapping(Array("/"))
class MetadataController {

  @Value("${menas.ui.version}")
  val version: String = ""

  @Value("${menas.api.url}")
  val apiURL: String = ""

  @GetMapping(value = Array("/metadata"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
  def getMetadata(): String = s"""{"version": "${version}", "apiUrl": "${apiURL}"}"""

  @GetMapping(path = Array("/lineageExecutionIdApiTemplate"))
  def getLineageExecutionIdApiTemplate(): String = {
    LineageConfig.executionIdApiTemplate.getOrElse("")
  }

}
