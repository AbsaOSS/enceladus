package za.co.absa.enceladus.menas

import com.google.common.base.Predicate
import org.springframework.context.annotation.{Bean, Configuration}
import springfox.documentation.builders.RequestHandlerSelectors
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import com.google.common.base.Predicates.or
import springfox.documentation.builders.PathSelectors.regex

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import com.google.common.base.Predicate
import springfox.documentation.builders.ApiInfoBuilder
import springfox.documentation.service.ApiInfo
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import springfox.documentation.builders.PathSelectors.regex
import com.google.common.base.Predicates.or


@Configuration
@EnableSwagger2
class SpringFoxConfig {
  @Bean
  def api(): Docket = {
    new Docket(DocumentationType.SWAGGER_2)
      .select
      .apis(RequestHandlerSelectors.any)
      .paths(filteredPaths)
      .build
  }

  private def filteredPaths: Predicate[String] =
    or[String](regex("/api/dataset.*"), regex("/api/schema.*"))
}
