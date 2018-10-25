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

package za.co.absa.enceladus.rest

import com.typesafe.config.ConfigFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.core.annotation.Order
import org.springframework.core.io.FileSystemResource
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.{EnableWebSecurity, WebSecurityConfigurerAdapter}
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator
import org.springframework.security.kerberos.client.config.SunJaasKrb5LoginConfig
import org.springframework.security.kerberos.client.ldap.KerberosLdapContextSource
import org.springframework.security.kerberos.web.authentication.{SpnegoAuthenticationProcessingFilter, SpnegoEntryPoint}
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch
import org.springframework.security.ldap.userdetails.{LdapUserDetailsMapper, LdapUserDetailsService}

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

  @Bean
  def activeDirectoryLdapAuthenticationProvider() = {
    new ActiveDirectoryLdapAuthenticationProvider(adDomain, adServer)
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
    ticketValidator
  }

  @Bean
  def kerberosLdapContextSource() = {
    val contextSource = new KerberosLdapContextSource(adServer)
    contextSource.setLoginConfig(loginConfig())
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

  @Configuration
  @Order(1)
  class ApiWebSecurityConfigurationAdapter() extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      http
        .authorizeRequests()
        .antMatchers("/resources/**", "/generic/**", "/service/**")
        .permitAll()
        .anyRequest().authenticated()
        .and
        .formLogin()
        .successForwardUrl("/index.jsp")
        .defaultSuccessUrl("/index.jsp")
        .permitAll()
        //        .and
        //        .httpBasic()
        .and()
        .logout()
        .logoutUrl("/logout")
        .permitAll()
        .clearAuthentication(true)
        .deleteCookies("JSESSIONID")
        .invalidateHttpSession(true)
    }

    override def configure(auth: AuthenticationManagerBuilder) {
      val conf = ConfigFactory.load()

      auth.authenticationProvider(activeDirectoryLdapAuthenticationProvider())
        .authenticationProvider(kerberosServiceAuthenticationProvider())
    }

    @Bean
    override def authenticationManagerBean() = {
      super.authenticationManagerBean()
    }
  }

}