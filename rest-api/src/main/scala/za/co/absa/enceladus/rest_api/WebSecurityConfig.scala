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

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.{EnableWebSecurity, WebSecurityConfigurerAdapter}
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.kerberos.web.authentication.{SpnegoAuthenticationProcessingFilter, SpnegoEntryPoint}
import org.springframework.security.web.authentication._
import za.co.absa.enceladus.rest_api.auth._
import za.co.absa.enceladus.rest_api.auth.jwt.JwtAuthenticationFilter
import za.co.absa.enceladus.rest_api.auth.kerberos.RestApiKerberosAuthentication


@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
class WebSecurityConfig @Autowired()(beanFactory: BeanFactory,
                                     jwtAuthFilter: JwtAuthenticationFilter,
                                     @Value("${enceladus.rest.auth.mechanism:}")
                                     authMechanism: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  @Configuration
  class ApiWebSecurityConfigurationAdapter @Autowired()(authenticationFailureHandler: AuthenticationFailureHandler,
                                                        authenticationSuccessHandler: AuthenticationSuccessHandler)
    extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      val kerberosFilter = RestApiKerberosAuthentication
        .spnegoAuthenticationProcessingFilter(authenticationManager, authenticationSuccessHandler)

      http
        .cors().and()
        .csrf()
          .disable()
        .sessionManagement()
          .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
        .and()
        .exceptionHandling()
          .authenticationEntryPoint(spnegoEntryPoint())
        .and()
        .authorizeRequests()
          .antMatchers(
            "/v3/api-docs*", "/v3/api-docs/**",
            "/v3/api-docs.yaml*","/v3/api-docs.yaml/**",
            "/admin/health",
            "/api/user/version", "/api/configuration/**",
            "/webjars/**",
            "/swagger-ui.html",  "/swagger-ui/**",
            "/swagger-resources/**"
          )
          .permitAll()
        .anyRequest()
          .authenticated()
        .and()
          .formLogin()
          .loginProcessingUrl("/api/login")
          .successHandler(authenticationSuccessHandler)
          .failureHandler(authenticationFailureHandler)
          .permitAll()
        .and()
          .addFilterBefore(kerberosFilter, classOf[UsernamePasswordAuthenticationFilter])
          .addFilterAfter(jwtAuthFilter, classOf[SpnegoAuthenticationProcessingFilter])
    }

    @Bean
    def spnegoEntryPoint(): SpnegoEntryPoint = {
      new SpnegoEntryPoint("/api/login")
    }

    override def configure(auth: AuthenticationManagerBuilder) {
      this.getMenasAuthentication().configure(auth)
    }

    private def getMenasAuthentication(): RestApiAuthentication = {
      authMechanism.toLowerCase match {
        case "inmemory" =>
          logger.info("Using InMemory REST API authentication")
          beanFactory.getBean(classOf[InMemoryRestApiAuthentication])
        case "kerberos" =>
          logger.info("Using Kerberos REST API Authentication")
          beanFactory.getBean(classOf[RestApiKerberosAuthentication])
        case _          =>
          throw new IllegalArgumentException("Invalid authentication mechanism - use one of: inmemory, kerberos")
      }
    }

    @Bean
    override def authenticationManagerBean(): AuthenticationManager = {
      super.authenticationManagerBean()
    }
  }

}
