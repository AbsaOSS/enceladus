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

import io.swagger.v3.oas.models.{Components, OpenAPI, Operation, PathItem}
import io.swagger.v3.oas.models.info.{Info, License}
import io.swagger.v3.oas.models.media.{Content, MediaType, Schema}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.security.SecurityScheme
import org.springdoc.core.GroupedOpenApi
import org.springframework.context.annotation.{Bean, Configuration}
import za.co.absa.enceladus.utils.general.ProjectMetadata

@Configuration
class SwaggerConfig extends ProjectMetadata {

  import scala.collection.JavaConverters._

  @Bean
  def api(): OpenAPI = {
    val apiInfo: Info = new Info()
      .title(s"Enceladus API")
      .description("Enceladus API reference")
      .version(projectVersion) // api or project?
      .license(new License()
        .name("Apache 2.0")
        .url("https://www.apache.org/licenses/LICENSE-2.0.html")
      )

    val components: Components = new Components()
      .addSecuritySchemes(
        "JWT", new SecurityScheme()
          .`type`(SecurityScheme.Type.APIKEY) // custom JWT header (no bearer wrapper)
          .`in`(SecurityScheme.In.HEADER)
          .name("JWT")
          .description("User token obtained at /api/login in JWT response header")
      )

    val loginPath: PathItem = new PathItem()
      .post(new Operation()
        .tags(List("authentication").asJava)
        .summary("Login")
        .description("Login with the given credentials.")
        .operationId("login")
        .parameters(List(
          new Parameter().name("username").in("query").required(true).schema(new Schema().`type`("string")),
          new Parameter().name("password").in("query").required(true)
            .schema(new Schema().`type`("string").format("password"))
        ).asJava)
        .responses(new ApiResponses()
          .addApiResponse("200", new ApiResponse().description("OK"))
          .addApiResponse("401", new ApiResponse().description("Unauthorized"))
        )
      )

    new OpenAPI()
      .info(apiInfo) // paths are based on groups
      .components(components)
      .path("/api/login", loginPath)
  }

  @Bean
  def prodApi: GroupedOpenApi = {
    GroupedOpenApi.builder()
      .group("latest-api")
      .pathsToMatch(prodPaths: _ *)
      .displayName("Enceladus V3 API")
      .build
  }

  @Bean
  def devApi: GroupedOpenApi = {
    GroupedOpenApi.builder()
      .group("legacy-and-latest-api")
      .displayName("Enceladus V2+3 API (dev)")
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


}
