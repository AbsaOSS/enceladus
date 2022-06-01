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

package za.co.absa.enceladus.rest_api.auth.kerberos

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.core.io.FileSystemResource
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.core.AuthenticationException
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator
import org.springframework.security.kerberos.client.config.SunJaasKrb5LoginConfig
import org.springframework.security.kerberos.client.ldap.KerberosLdapContextSource
import org.springframework.security.kerberos.web.authentication.SpnegoAuthenticationProcessingFilter
import org.springframework.security.ldap.userdetails.{LdapUserDetailsMapper, LdapUserDetailsService}
import org.springframework.security.web.authentication.{AuthenticationFailureHandler, AuthenticationSuccessHandler}
import org.springframework.stereotype.Component
import za.co.absa.enceladus.rest_api.auth.MenasAuthentication
import za.co.absa.enceladus.rest_api.auth.kerberos.MenasKerberosAuthentication._

@Component("kerberosMenasAuthentication")
class MenasKerberosAuthentication @Autowired()(@Value("${menas.auth.ad.domain:}")
                                               adDomain: String,
                                               @Value("${menas.auth.ad.server:}")
                                               adServer: String,
                                               @Value("${menas.auth.servicename.principal:}")
                                               servicePrincipal: String,
                                               @Value("${menas.auth.servicename.keytab.location:}")
                                               keytabLocation: String,
                                               @Value("${menas.auth.ldap.search.base:}")
                                               ldapSearchBase: String,
                                               @Value("${menas.auth.ldap.search.filter:}")
                                               ldapSearchFilter: String,
                                               @Value("${menas.auth.kerberos.debug:false}")
                                               kerberosDebug: Boolean,
                                               @Value("${menas.auth.kerberos.krb5conf:}")
                                               krb5conf: String)
  extends MenasAuthentication with InitializingBean {

  private lazy val requiredParameters = Seq((adDomain, "menas.auth.ad.domain"),
    (adServer, "menas.auth.ad.server"),
    (servicePrincipal, "menas.auth.servicename.principal"),
    (keytabLocation, "menas.auth.servicename.keytab.location"),
    (ldapSearchBase, "menas.auth.ldap.search.base"),
    (ldapSearchFilter, "menas.auth.ldap.search.filter"))

  override def afterPropertiesSet() {
    System.setProperty("javax.net.debug", kerberosDebug.toString)
    System.setProperty("sun.security.krb5.debug", kerberosDebug.toString)

    if (!krb5conf.isEmpty) {
      logger.info(s"Using KRB5 CONF from $krb5conf")
      System.setProperty("java.security.krb5.conf", krb5conf)
    }
  }

  private def validateParam(param: String, paramName: String): Unit = {
    if (param.isEmpty) {
      throw new IllegalArgumentException(s"$paramName has to be configured in order to use kerberos Menas authentication")
    }
  }

  private def validateParams() {
    requiredParameters.foreach(p => this.validateParam(p._1, p._2))
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
    loginConfig.setServicePrincipal(servicePrincipal)
    loginConfig.setKeyTabLocation(new FileSystemResource(keytabLocation))
    loginConfig.setDebug(kerberosDebug)
    loginConfig.setIsInitiator(true)
    loginConfig.setUseTicketCache(false)
    loginConfig.afterPropertiesSet()
    loginConfig
  }

  private def kerberosLdapContextSource() = {
    val contextSource = new KerberosLdapContextSource(adServer)
    contextSource.setLoginConfig(loginConfig())
    contextSource.afterPropertiesSet()
    contextSource
  }

  private def ldapUserDetailsService() = {
    val userSearch = new KerberosLdapUserSearch(ldapSearchBase, ldapSearchFilter, kerberosLdapContextSource())
    val service = new LdapUserDetailsService(userSearch, new ActiveDirectoryLdapAuthoritiesPopulator())
    service.setUserDetailsMapper(new LdapUserDetailsMapper())
    service
  }

  private def kerberosServiceAuthenticationProvider() = {
    val provider = new KerberosServiceAuthenticationProvider()
    provider.setTicketValidator(sunJaasKerberosTicketValidator())
    provider.setUserDetailsService(ldapUserDetailsService())
    provider
  }

  override def configure(auth: AuthenticationManagerBuilder): Unit = {
    this.validateParams()
    val originalLogLevel = Logger.getRootLogger.getLevel
    //something here changes the log level to WARN
    auth
      .authenticationProvider(new MenasKerberosAuthenticationProvider(adServer, ldapSearchFilter, ldapSearchBase))
      .authenticationProvider(kerberosServiceAuthenticationProvider())
    Logger.getRootLogger.setLevel(originalLogLevel)
  }
}

object MenasKerberosAuthentication {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def spnegoAuthenticationProcessingFilter(authenticationManager: AuthenticationManager,
                                           authenticationSuccessHandler: AuthenticationSuccessHandler): SpnegoAuthenticationProcessingFilter = {
    val filter = new SpnegoAuthenticationProcessingFilter()
    filter.setAuthenticationManager(authenticationManager)
    filter.setSkipIfAlreadyAuthenticated(true)
    filter.setSuccessHandler(authenticationSuccessHandler)
    filter.setFailureHandler(new AuthenticationFailureHandler {
      override def onAuthenticationFailure(request: HttpServletRequest, response: HttpServletResponse, exception: AuthenticationException): Unit = {
        logger.error(exception.getStackTrace.toString)
      }
    })
    filter
  }
}
