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

package za.co.absa.enceladus.menas.auth.kerberos

import org.slf4j.LoggerFactory
import org.springframework.security.authentication.AuthenticationProvider
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.ldap.userdetails.LdapUserDetailsService

import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext

class MenasKerberosAuthenticationProvider(adServer: String, searchFilter: String, baseDN: String)
  extends AuthenticationProvider {

  case class MenasKerberosLoginResult(loginContext: LoginContext, verifiedName: String)

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def authenticate(authentication: Authentication): Authentication = {
    val auth = authentication.asInstanceOf[UsernamePasswordAuthenticationToken]
    val loginResult = login(auth.getName, auth.getCredentials.toString)
    val userDetailsService = getUserDetailService(loginResult.loginContext.getSubject)
    val userDetails = userDetailsService.loadUserByUsername(loginResult.verifiedName)
    loginResult.loginContext.logout()
    val output = new UsernamePasswordAuthenticationToken(userDetails, auth.getCredentials, userDetails.getAuthorities)
    output.setDetails(authentication.getDetails)
    output
  }

  override def supports(authentication: Class[_]): Boolean = {
    classOf[UsernamePasswordAuthenticationToken].isAssignableFrom(authentication)
  }

  private def login(username: String, password: String): MenasKerberosLoginResult = {
    val loginContext = new LoginContext("", null, getSpringCBHandler(username, password), getLoginConfig) // scalastyle:ignore null
    loginContext.login()
    val loggedInUser = loginContext.getSubject.getPrincipals.iterator.next.toString
    logger.debug(s"Logged In User: $loggedInUser")
    MenasKerberosLoginResult(loginContext, loggedInUser)
  }

  private def getSpringCBHandler(username: String, password: String) = {
    new CallbackHandler(){
      def handle(callbacks: Array[Callback]) {
        callbacks.foreach({
            case ncb: NameCallback => ncb.setName(username)
            case pwdcb: PasswordCallback => pwdcb.setPassword(password.toCharArray)
        })
      }
    }
  }

  private def getUserDetailService(subject: Subject) = {
    val contextSource = new MenasKerberosLdapContextSource(adServer, subject)
    contextSource.afterPropertiesSet()
    val userSearch = new KerberosLdapUserSearch(baseDN, searchFilter, contextSource)
    new LdapUserDetailsService(userSearch, new ActiveDirectoryLdapAuthoritiesPopulator())
  }

  private def getLoginConfig: Configuration = {
    import scala.collection.JavaConversions._

    new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
        val opts = Map(
          "storeKey" -> "true",
          "refreshKrb5Config" -> "true",
          "isInitiator" -> "true",
          "debug" -> "true")
        Array(new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, opts))
      }
    }
  }
}
