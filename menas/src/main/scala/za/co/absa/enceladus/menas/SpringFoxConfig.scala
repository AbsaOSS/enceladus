package za.co.absa.enceladus.menas

import springfox.documentation.builders.RequestHandlerSelectors

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import com.google.common.base.Predicate
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import springfox.documentation.builders.PathSelectors.regex
import com.google.common.base.Predicates.or
import springfox.documentation.builders.ApiInfoBuilder

@Configuration
@EnableSwagger2
class SpringFoxConfig {
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
    or[String](regex("/api/dataset.*"), regex("/api/schema.*"))

  private def apiInfo =
    new ApiInfoBuilder()
      .title("Menas API")
      .description("Menas API reference for developers")
      .license("Apache 2.0 License")
      .version("1.0") // api or project?
      .build
}
