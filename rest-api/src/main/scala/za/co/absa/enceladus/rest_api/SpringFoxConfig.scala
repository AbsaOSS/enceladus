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

import com.google.common.base.Predicate
import com.google.common.base.Predicates.or
import org.springframework.context.annotation.{Bean, Configuration}
import springfox.documentation.builders.PathSelectors.regex
import springfox.documentation.builders.{ApiInfoBuilder, RequestHandlerSelectors}
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import za.co.absa.enceladus.utils.general.ProjectMetadata

@Configuration
@EnableSwagger2
class SpringFoxConfig extends ProjectMetadata {
  @Bean
  def api(): Docket = {
    new Docket(DocumentationType.SWAGGER_2)
      .apiInfo(apiInfo)
      .select
      .apis(RequestHandlerSelectors.any)
      .paths(filteredPaths)
      .build
  }

  private def filteredPaths: Predicate[String] =
    or[String](regex("/api/dataset.*"), regex("/api/schema.*"),
      regex("/api/mappingTable.*"), regex("/api/properties.*"),
      regex("/api/monitoring.*"),regex("/api/runs.*"),
      regex("/api/user.*"), regex("/api/spark.*"),
      regex("/api/configuration.*")
    )

  private def apiInfo =
    new ApiInfoBuilder()
      .title("Menas API")
      .description("Menas API reference for developers")
      .license("Apache 2.0 License")
      .version(projectVersion) // api or project?
      .build
}
