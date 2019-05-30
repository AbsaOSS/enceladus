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

package za.co.absa.enceladus.menas.auth

import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.FileSystemResource
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator
import org.springframework.security.kerberos.client.config.SunJaasKrb5LoginConfig
import org.springframework.security.kerberos.client.ldap.KerberosLdapContextSource
import org.springframework.security.kerberos.web.authentication.SpnegoAuthenticationProcessingFilter
import org.springframework.security.kerberos.web.authentication.SpnegoEntryPoint
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch
import org.springframework.security.ldap.userdetails.LdapUserDetailsMapper
import org.springframework.security.ldap.userdetails.LdapUserDetailsService
import org.springframework.stereotype.Component

import za.co.absa.enceladus.menas.ActiveDirectoryLdapAuthoritiesPopulator

@Component("kerberosMenasAuthentication")
class KerberosMenasAuthentication() extends MenasAuthentication with InitializingBean {
  @Value("${za.co.absa.enceladus.menas.auth.ad.domain:}")
  val adDomain: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ad.server:}")
  val adServer: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.sysuser.principal:}")
  val servicePrincipal: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.sysuser.keytab.location:}")
  val keytabLocation: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ldap.search.base:}")
  val ldapSearchBase: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.ldap.search.filter:}")
  val ldapSearchFilter: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.kerberos.debug:false}")
  val kerberosDebug: Boolean = false

  lazy val requiredParameters = Seq((adDomain, "za.co.absa.enceladus.menas.auth.ad.domain"),
    (adServer, "za.co.absa.enceladus.menas.auth.ad.server"),
    (servicePrincipal, "za.co.absa.enceladus.menas.auth.sysuser.principal"),
    (keytabLocation, "za.co.absa.enceladus.menas.auth.sysuser.keytab.location"),
    (ldapSearchBase, "za.co.absa.enceladus.menas.auth.ldap.search.base"),
    (ldapSearchFilter, "za.co.absa.enceladus.menas.auth.ldap.search.filter"))

  override def afterPropertiesSet() {
    System.setProperty("javax.net.debug", kerberosDebug.toString)
    System.setProperty("sun.security.krb5.debug", kerberosDebug.toString)
  }

  private def validateParam(param: String, paramName: String) = {
    if (param.isEmpty()) throw new IllegalArgumentException(s"$paramName has to be configured in order to use kerberos Menas authentication")
  }

  private def validateParams() {
    requiredParameters.foreach(p => this.validateParam(p._1, p._2))
  }

  private def activeDirectoryLdapAuthenticationProvider() = {
    val prov = new ActiveDirectoryLdapAuthenticationProvider(adDomain, adServer, ldapSearchBase)
    prov.setSearchFilter(ldapSearchFilter)
    prov.setUseAuthenticationRequestCredentials(true)
    prov.setConvertSubErrorCodesToExceptions(true)
    prov
  }

  private def spnegoEntryPoint() = {
    new SpnegoEntryPoint("/login")
  }

  private def spnegoAuthenticationProcessingFilter(authenticationManager: AuthenticationManager) = {
    val filter = new SpnegoAuthenticationProcessingFilter()
    filter.setAuthenticationManager(authenticationManager)
    filter
  }

  private def sunJaasKerberosTicketValidator() = {
    val ticketValidator = new SunJaasKerberosTicketValidator()
    ticketValidator.setServicePrincipal(servicePrincipal)
    ticketValidator.setKeyTabLocation(new FileSystemResource(keytabLocation))
    ticketValidator.setDebug(kerberosDebug)
    ticketValidator.afterPropertiesSet()
    ticketValidator
  }

  private def loginConfig() = {
    val loginConfig = new SunJaasKrb5LoginConfig()
    loginConfig.setKeyTabLocation(new FileSystemResource(keytabLocation))
    loginConfig.setServicePrincipal(servicePrincipal)
    loginConfig.setDebug(kerberosDebug)
    loginConfig.setIsInitiator(true)
    loginConfig.afterPropertiesSet()
    loginConfig
  }

  private def kerberosLdapContextSource() = {
    val contextSource = new KerberosLdapContextSource(adServer)
    contextSource.setLoginConfig(loginConfig())
    contextSource
  }

  private def ldapUserDetailsService() = {
    val userSearch = new FilterBasedLdapUserSearch(ldapSearchBase, ldapSearchFilter, kerberosLdapContextSource());

    val service = new LdapUserDetailsService(userSearch, new ActiveDirectoryLdapAuthoritiesPopulator());
    service.setUserDetailsMapper(new LdapUserDetailsMapper())
    service
  }

  private def kerberosServiceAuthenticationProvider() = {
    val provider = new KerberosServiceAuthenticationProvider()
    provider.setTicketValidator(sunJaasKerberosTicketValidator())
    provider.setUserDetailsService(ldapUserDetailsService())
    provider
  }

  override def configure(auth: AuthenticationManagerBuilder) {
    this.validateParams()
    auth.authenticationProvider(kerberosServiceAuthenticationProvider()).authenticationProvider(activeDirectoryLdapAuthenticationProvider())
  }

}
