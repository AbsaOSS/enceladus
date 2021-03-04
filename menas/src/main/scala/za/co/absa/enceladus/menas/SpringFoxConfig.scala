package za.co.absa.enceladus.menas

import org.springframework.context.annotation.{Bean, Configuration}
import springfox.documentation.builders.PathSelectors
import springfox.documentation.builders.RequestHandlerSelectors
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2

@Configuration
@EnableSwagger2
class SpringFoxConfig {
  @Bean
  def api(): Docket = {
    new Docket(DocumentationType.SWAGGER_2)
      .select
      .apis(RequestHandlerSelectors.any)
      .paths(PathSelectors.any)
      .build
  }
}
