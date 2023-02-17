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

package za.co.absa.enceladus.rest_api

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.{Info, License}
import org.springdoc.core.GroupedOpenApi
import org.springframework.context.annotation.{Bean, Configuration}
import za.co.absa.enceladus.utils.general.ProjectMetadata

@Configuration
class SpringFoxConfig extends ProjectMetadata {

  import org.springframework.beans.factory.annotation.Value

  @Value("${spring.profiles.active:}")
  private val activeProfiles: String = null

  @Bean
  def api(): OpenAPI = {
    val isDev = activeProfiles.split(",").map(_.toLowerCase).contains("dev")

    new OpenAPI()
      .info(apiInfo(isDev))
    // paths are now selectable using different groups
}

  @Bean
  def prodApi: GroupedOpenApi = {
    GroupedOpenApi.builder()
      .group("prod-api")
      .pathsToMatch(prodPaths: _ *)
      .build
  }
  @Bean
  def devApi: GroupedOpenApi =  {
    GroupedOpenApi.builder()
      .group("dev-api")
      .pathsToMatch(devPaths: _ *)
      .build
  }

  private val v2Paths = Seq(
    "/api/dataset/**", "/api/schema/**",
    "/api/mappingTable/**", "/api/properties/**",
    "/api/monitoring/**", "/api/runs/**",
    "/api/user/**", "/api/spark/**",
    "/api/configuration/**"
  )

  private val v3paths = Seq(
    "/api-v3/datasets/**", "/api-v3/schemas/**",
    "/api-v3/mapping-tables/**", "/api-v3/property-definitions/**",
    "/api-v3/runs/**"
  )

  private val devPaths = v2Paths ++ v3paths
  private val prodPaths = v3paths


  private def apiInfo(isDev: Boolean): Info = {
    new Info()
      .title(s"Enceladus API")
      .description("Enceladus API reference")
      .version(projectVersion) // api or project?
      .license(new License()
          .name("Apache 2.0")
          .url("https://www.apache.org/licenses/LICENSE-2.0.html")
      )
  }
}
