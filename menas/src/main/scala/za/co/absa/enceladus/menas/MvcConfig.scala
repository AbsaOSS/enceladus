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

import org.springframework.context.annotation.Configuration
import org.springframework.format.FormatterRegistry
import org.springframework.web.servlet.config.annotation.{CorsRegistry, ViewControllerRegistry, WebMvcConfigurer}
import za.co.absa.enceladus.menas.auth.AuthConstants.{CsrfTokenKey, JwtKey}

@Configuration
class MvcConfig extends WebMvcConfigurer {
  override def addViewControllers(registry: ViewControllerRegistry) {
    registry.addViewController("/login").setViewName("login")
  }

  override def addCorsMappings(registry: CorsRegistry): Unit = {
    registry.addMapping("/**")
      .exposedHeaders(JwtKey, CsrfTokenKey)
      .allowedMethods("PUT", "GET", "DELETE", "OPTIONS", "PATCH", "POST")
      .allowedHeaders("*")
      .allowedOrigins("*")
  }

  override def addFormatters(registry: FormatterRegistry): Unit = {
    registry.addConverter(new StringToValidationKindConverter)
  }
}
