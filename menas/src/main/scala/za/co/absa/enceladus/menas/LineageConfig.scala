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

package za.co.absa.enceladus.menas

import za.co.absa.enceladus.utils.config.ConfigReader

object LineageConfig {
  final val apiUrl: Option[String] = new ConfigReader().readStringConfigIfExist("menas.lineage.readApiUrl").filter(_.trim.nonEmpty)
  final val jarName =  "spline"
  final val subSpace = "lineage"

  final val mappingPathForController = "/" + subSpace + "/app/**"
  final val resourceHandler = "/" + subSpace + "/**"
  final val resourceLocation = "/webjars/" + jarName + "/"
  final val executionIdApiTemplate: Option[String] = apiUrl.map(_ + "/execution-events?dataSourceUri=%s&applicationId=%s")

  def baseUrl(baseUrlPrefix: String): String = {
    val delimiter  = if (baseUrlPrefix.takeRight(1) == "/") "" else "/"
    s"$baseUrlPrefix$delimiter$subSpace/"
  }
}
