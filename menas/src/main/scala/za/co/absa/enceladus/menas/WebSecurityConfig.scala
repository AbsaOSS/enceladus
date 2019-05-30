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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.core.annotation.Order
import org.springframework.core.io.FileSystemResource
import org.springframework.http.HttpStatus
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.{EnableWebSecurity, WebSecurityConfigurerAdapter}
import org.springframework.security.core.{Authentication, AuthenticationException}
import org.springframework.security.crypto.password.NoOpPasswordEncoder
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator
import org.springframework.security.kerberos.client.config.SunJaasKrb5LoginConfig
import org.springframework.security.kerberos.client.ldap.KerberosLdapContextSource
import org.springframework.security.kerberos.web.authentication.{SpnegoAuthenticationProcessingFilter, SpnegoEntryPoint}
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch
import org.springframework.security.ldap.userdetails.{LdapUserDetailsMapper, LdapUserDetailsService}
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.security.web.authentication.logout.{HttpStatusReturningLogoutSuccessHandler, LogoutSuccessHandler}
import org.springframework.security.web.authentication.{AuthenticationFailureHandler, SimpleUrlAuthenticationFailureHandler, SimpleUrlAuthenticationSuccessHandler}
import org.springframework.security.web.csrf.CsrfToken
import org.springframework.security.web.savedrequest.{HttpSessionRequestCache, RequestCache, SavedRequest}
import org.springframework.stereotype.Component
import org.springframework.util.StringUtils
import org.springframework.security.kerberos.authentication.sun.GlobalSunJaasKerberosConfig
import org.apache.log4j.Logger
import org.apache.log4j.Level

@EnableWebSecurity
class WebSecurityConfig {

  @Value("${za.co.absa.enceladus.menas.auth.ad.domain}")
  val adDomain: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ad.server}")
  val adServer: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.sysuser.principal}")
  val servicePrincipal: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.sysuser.keytab.location}")
  val keytabLocation: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ldap.search.base}")
  val ldapSearchBase: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ldap.search.filter}")
  val ldapSearchFilter: String = ""

  // Test-only in-memory auth
  @Value("${za.co.absa.enceladus.menas.auth.inmemory.user}")
  val username: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.inmemory.password}")
  val password: String = ""

  System.setProperty("javax.net.ssl.trustStore", "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/lib/security/cacerts");
  System.setProperty("javax.net.debug", "true")
  System.setProperty("sun.security.krb5.debug", "true")

  @Bean
  def activeDirectoryLdapAuthenticationProvider() = {
    val prov = new ActiveDirectoryLdapAuthenticationProvider(adDomain, adServer, ldapSearchBase)
    prov.setSearchFilter(ldapSearchFilter)
    prov
  }

  @Bean
  def spnegoEntryPoint() = {
    new SpnegoEntryPoint("/login")
  }

  @Bean
  def spnegoAuthenticationProcessingFilter(authenticationManager: AuthenticationManager) = {
    val filter = new SpnegoAuthenticationProcessingFilter()
    filter.setAuthenticationManager(authenticationManager)
    
    filter
  }

  @Bean
  def kerberosServiceAuthenticationProvider() = {
    val provider = new KerberosServiceAuthenticationProvider()
    provider.setTicketValidator(sunJaasKerberosTicketValidator())
    provider.setUserDetailsService(ldapUserDetailsService())
    provider
  }

  @Bean
  def sunJaasKerberosTicketValidator() = {
    val ticketValidator = new SunJaasKerberosTicketValidator()
    ticketValidator.setServicePrincipal(servicePrincipal)
    ticketValidator.setKeyTabLocation(new FileSystemResource(keytabLocation))
    ticketValidator.setDebug(true)
    ticketValidator.afterPropertiesSet()
    ticketValidator
  }

  @Bean
  def kerberosLdapContextSource() = {
    val contextSource = new KerberosLdapContextSource(adServer)
    contextSource.setLoginConfig(loginConfig())
//    contextSource.setBase(ldapSearchBase)
    contextSource.setUserDn("OU=Users,OU=CORP Accounts,DC=corp,DC=dsarena,DC=com")
    contextSource
  }

  @Bean
  def loginConfig() = {
    val loginConfig = new SunJaasKrb5LoginConfig()
    loginConfig.setKeyTabLocation(new FileSystemResource(keytabLocation))
    loginConfig.setServicePrincipal(servicePrincipal)
    loginConfig.setDebug(true)
    loginConfig.setIsInitiator(true)
    loginConfig.afterPropertiesSet()
    loginConfig
  }

  @Bean
  def ldapUserDetailsService() = {
    val userSearch = new FilterBasedLdapUserSearch(ldapSearchBase, ldapSearchFilter, kerberosLdapContextSource());

    val service = new LdapUserDetailsService(userSearch, new ActiveDirectoryLdapAuthoritiesPopulator());
    service.setUserDetailsMapper(new LdapUserDetailsMapper())
    service
  }

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
  class ApiWebSecurityConfigurationAdapter @Autowired() (restAuthenticationEntyPoint: RestAuthenticationEntryPoint,
      authenticationSuccessHandler: AuthSuccessHandler,
      authenticationFailureHandler: AuthenticationFailureHandler,
      logoutSuccessHandler: LogoutSuccessHandler)
    extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      http
        .csrf()
          .ignoringAntMatchers("/api/login")
        .and()
          .exceptionHandling()
          .authenticationEntryPoint(restAuthenticationEntyPoint)
        .and()
          .authorizeRequests()
          .antMatchers("/index.html", "/resources/**", "/generic/**",
            "/service/**", "/webjars/**", "/css/**", "/components/**")
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
    }

//    override def configure(auth: AuthenticationManagerBuilder) {
//      auth
//        .inMemoryAuthentication()
//        .passwordEncoder(NoOpPasswordEncoder.getInstance())
//        .withUser(username)
//        .password(password)
//        .authorities("ROLE_USER")
//    }

    override def configure(auth: AuthenticationManagerBuilder) {
       val res = auth.authenticationProvider(kerberosServiceAuthenticationProvider()).authenticationProvider(activeDirectoryLdapAuthenticationProvider())
       Logger.getRootLogger().setLevel(Level.DEBUG);
    }

    @Bean
    override def authenticationManagerBean() = {
      super.authenticationManagerBean()
    }
  }

  @Component
  class RestAuthenticationEntryPoint extends AuthenticationEntryPoint {
    override def commence(request: HttpServletRequest,
        response: HttpServletResponse,
        authException: AuthenticationException): Unit = {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, s"Unauthorized")
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
