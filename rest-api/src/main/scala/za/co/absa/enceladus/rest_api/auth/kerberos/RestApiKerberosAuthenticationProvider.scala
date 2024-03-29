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

import java.net.{SocketTimeoutException, UnknownHostException}
import java.security.PrivilegedActionException

import javax.security.auth.Subject
import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback}
import javax.security.auth.login.{AppConfigurationEntry, Configuration, LoginContext, LoginException}
import org.slf4j.LoggerFactory
import org.springframework.security.authentication.{AuthenticationProvider, BadCredentialsException, UsernamePasswordAuthenticationToken}
import org.springframework.security.core.Authentication
import org.springframework.security.ldap.userdetails.LdapUserDetailsService
import sun.security.krb5.KrbException
import za.co.absa.enceladus.rest_api.auth.exceptions.{AuthHostTimeoutException, BadKrbHostException, BadLdapHostException}

import scala.util.control.NonFatal

class RestApiKerberosAuthenticationProvider(adServer: String, searchFilter: String, baseDN: String)
  extends AuthenticationProvider {

  case class RestApiKerberosLoginResult(loginContext: LoginContext, verifiedName: String)

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def authenticate(authentication: Authentication): Authentication = {
    val output = try {
      val auth = authentication.asInstanceOf[UsernamePasswordAuthenticationToken]
      val loginResult = login(auth.getName, auth.getCredentials.toString)
      val userDetailsService = getUserDetailService(loginResult.loginContext.getSubject)
      val userDetails = userDetailsService.loadUserByUsername(loginResult.verifiedName)
      loginResult.loginContext.logout()
      new UsernamePasswordAuthenticationToken(userDetails, auth.getCredentials, userDetails.getAuthorities)
    } catch {
      case ex: LoginException =>
        ex.getCause match {
          // This is thrown when there is an issue contacting a KDC specified in krb5.conf for the realm
          case nestedException: SocketTimeoutException =>
            throw AuthHostTimeoutException(s"Timeout host: ${nestedException.getMessage}", ex)
          case nestedException: UnknownHostException =>
            throw BadKrbHostException(s"Unknown authentication host: ${nestedException.getMessage}", ex)
          case nestedException: KrbException =>
            throw new BadCredentialsException(s"Invalid credentials: ${nestedException.getMessage}", ex)
          case NonFatal(_) =>
            throw ex
        }
      // This is thrown when there is an issue contacting an LDAP server specified in REST API configuration
      case ex: PrivilegedActionException =>
        throw BadLdapHostException(ex.toString, ex)
      case NonFatal(ex) =>
        throw ex
    }
    output.setDetails(authentication.getDetails)
    output
  }

  override def supports(authentication: Class[_]): Boolean = {
    classOf[UsernamePasswordAuthenticationToken].isAssignableFrom(authentication)
  }

  private def login(username: String, password: String): RestApiKerberosLoginResult = {
    val loginContext = new LoginContext("", null, getSpringCBHandler(username, password), getLoginConfig) // scalastyle:ignore null
    loginContext.login()
    val loggedInUser = loginContext.getSubject.getPrincipals.iterator.next.toString
    logger.debug(s"Logged In User: $loggedInUser")
    RestApiKerberosLoginResult(loginContext, loggedInUser)
  }

  private def getSpringCBHandler(username: String, password: String) = {
    new CallbackHandler() {
      def handle(callbacks: Array[Callback]): Unit = {
        callbacks.foreach({
          case ncb: NameCallback => ncb.setName(username)
          case pwdcb: PasswordCallback => pwdcb.setPassword(password.toCharArray)
        })
      }
    }
  }

  private def getUserDetailService(subject: Subject) = {
    val contextSource = new RestApiKerberosLdapContextSource(adServer, subject)
    contextSource.afterPropertiesSet()
    val userSearch = new KerberosLdapUserSearch(baseDN, searchFilter, contextSource)
    new LdapUserDetailsService(userSearch, new ActiveDirectoryLdapAuthoritiesPopulator())
  }

  private def getLoginConfig: Configuration = {
    import scala.collection.JavaConverters._

    new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
        val opts = Map(
          "storeKey" -> "true",
          "refreshKrb5Config" -> "true",
          "isInitiator" -> "true",
          "debug" -> "true")
        Array(new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, opts.asJava))
      }
    }
  }
}
