/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.{EnableWebSecurity, WebSecurityConfigurerAdapter}
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.kerberos.web.authentication.{SpnegoAuthenticationProcessingFilter, SpnegoEntryPoint}
import org.springframework.security.web.authentication._
import za.co.absa.enceladus.menas.auth._
import za.co.absa.enceladus.menas.auth.jwt.JwtAuthenticationFilter
import za.co.absa.enceladus.menas.auth.kerberos.MenasKerberosAuthentication

@EnableWebSecurity
class WebSecurityConfig @Autowired()(beanFactory: BeanFactory,
                                     jwtAuthFilter: JwtAuthenticationFilter,
                                     @Value("${menas.auth.mechanism:}")
                                     authMechanism: String,
                                     @Value("${menas.version}")
                                     menasVersion: String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  @Configuration
  class ApiWebSecurityConfigurationAdapter @Autowired()(authenticationFailureHandler: AuthenticationFailureHandler,
                                                        authenticationSuccessHandler: AuthenticationSuccessHandler)
    extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      val kerberosFilter = MenasKerberosAuthentication
        .spnegoAuthenticationProcessingFilter(authenticationManager, authenticationSuccessHandler)

      http
        .csrf()
          .disable()
        .sessionManagement()
          .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
        .and()
        .exceptionHandling()
          .authenticationEntryPoint(spnegoEntryPoint())
        .and()
        .authorizeRequests()
          .antMatchers("/index.html", "/resources/**", "/generic/**",
            "/service/**", "/webjars/**", "/css/**", "/components/**", "/admin/health",
            "/api/oozie/isEnabled", "/api/user/version", s"/$menasVersion/**", "/api/configuration/**")
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
        .headers()
          .frameOptions()
          .sameOrigin()
    }

    @Bean
    def spnegoEntryPoint(): SpnegoEntryPoint = {
      new SpnegoEntryPoint("/api/login")
    }

    override def configure(auth: AuthenticationManagerBuilder) {
      this.getMenasAuthentication().configure(auth)
    }

    private def getMenasAuthentication(): MenasAuthentication = {
      authMechanism.toLowerCase match {
        case "inmemory" =>
          logger.info("Using InMemory Menas authentication")
          beanFactory.getBean(classOf[InMemoryMenasAuthentication])
        case "kerberos" =>
          logger.info("Using Kerberos Menas Authentication")
          beanFactory.getBean(classOf[MenasKerberosAuthentication])
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
