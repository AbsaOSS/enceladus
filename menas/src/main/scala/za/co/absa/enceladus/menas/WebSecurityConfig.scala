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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.annotation.Order
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter
import org.springframework.security.core.Authentication
import org.springframework.security.kerberos.web.authentication.SpnegoEntryPoint
import org.springframework.security.web.authentication.AuthenticationFailureHandler
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler
import org.springframework.security.web.csrf.CsrfToken
import org.springframework.stereotype.Component
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.springframework.security.authentication.AuthenticationManager
import za.co.absa.enceladus.menas.auth.InMemoryMenasAuthentication
import za.co.absa.enceladus.menas.auth.MenasAuthentication
import za.co.absa.enceladus.menas.auth.kerberos.MenasKerberosAuthentication

@EnableWebSecurity
class WebSecurityConfig {

  private val logger = LoggerFactory.getLogger(this.getClass)

  @Value("${za.co.absa.enceladus.menas.auth.mechanism:}")
  val authMechanism: String = ""

  @Value("${za.co.absa.enceladus.menas.version}")
  val menasVersion: String = ""

  @Autowired
  val beanFactory: BeanFactory = null // scalastyle:ignore null

  @Bean
  def authenticationFailureHandler(): AuthenticationFailureHandler = {
    new SimpleUrlAuthenticationFailureHandler()
  }

  @Bean
  def logoutSuccessHandler(): LogoutSuccessHandler = {
    new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK)
  }

  @Configuration
  @Order(1)
  class ApiWebSecurityConfigurationAdapter @Autowired() (authenticationSuccessHandler: AuthSuccessHandler,
      authenticationFailureHandler: AuthenticationFailureHandler,
      logoutSuccessHandler: LogoutSuccessHandler)
    extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      http
        .csrf()
          .ignoringAntMatchers("/api/login")
        .and()
        .exceptionHandling()
          .authenticationEntryPoint(spnegoEntryPoint())
        .and()
        .authorizeRequests()
          .antMatchers("/index.html", "/resources/**", "/generic/**",
            "/service/**", "/webjars/**", "/css/**", "/components/**",
            "/api/oozie/isEnabled", "/api/user/version", s"/$menasVersion/**")
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
          .logout()
          .logoutUrl("/api/logout")
          .logoutSuccessHandler(logoutSuccessHandler)
          .permitAll()
          .clearAuthentication(true)
          .deleteCookies("JSESSIONID")
          .invalidateHttpSession(true)
        .and
        .addFilterBefore(MenasKerberosAuthentication.spnegoAuthenticationProcessingFilter(authenticationManagerBean()), classOf[UsernamePasswordAuthenticationFilter])
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
        case "inmemory" => {
          logger.info("Using InMemory Menas authentication")
          beanFactory.getBean(classOf[InMemoryMenasAuthentication])
        }
        case "kerberos" => {
          logger.info("Using Kerberos Menas Authentication")
          beanFactory.getBean(classOf[MenasKerberosAuthentication])
        }
        case _ => {
          throw new IllegalArgumentException("Invalid authentication mechanism - use one of: inmemory, kerberos")
        }
      }
    }

    @Bean
    override def authenticationManagerBean(): AuthenticationManager = {
      super.authenticationManagerBean()
    }
  }

  @Component
  class AuthSuccessHandler extends SimpleUrlAuthenticationSuccessHandler {

    override def onAuthenticationSuccess(request: HttpServletRequest,
        response: HttpServletResponse,
        authentication: Authentication): Unit = {
      val csrfToken = request.getAttribute("_csrf").asInstanceOf[CsrfToken]
      response.addHeader(csrfToken.getHeaderName, csrfToken.getToken)

      clearAuthenticationAttributes(request)
    }

  }

}
