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

package za.co.absa.enceladus.rest_api.auth

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.authentication.configurers.provisioning.InMemoryUserDetailsManagerConfigurer
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.stereotype.Component

@Component
class InMemoryRestApiAuthentication extends RestApiAuthentication {
  @Value("${enceladus.rest.auth.inmemory.user:}")
  private val username: String = ""
  @Value("${enceladus.rest.auth.inmemory.password:}")
  private val password: String = ""
  @Value("${enceladus.rest.auth.inmemory.admin.user:}")
  private val adminUsername: String = ""
  @Value("${enceladus.rest.auth.inmemory.admin.password:}")
  private val adminPassword: String = ""
  @Value("${enceladus.rest.auth.admin.role}") // Spring throws errors if it is not here as well
  private val adminRole: String = ""

  protected val passwordEncoder = new BCryptPasswordEncoder()

  def validateParams(): Unit = {
    if (username.isEmpty || password.isEmpty) {
      throw new IllegalArgumentException("Both username and password have to configured for inmemory authentication.")
    }
  }

  override def configure(auth: AuthenticationManagerBuilder): Unit = {
    this.validateParams()

    val inMemoryAuth = auth
      .inMemoryAuthentication()
      .passwordEncoder(passwordEncoder)

    addUsers(inMemoryAuth)
  }

  protected def addUsers(auth: InMemoryUserDetailsManagerConfigurer[_]): Unit = {
    auth
      .withUser(username)
      .password(passwordEncoder.encode(password))
      .authorities("ROLE_USER")
      .and()
      .withUser(adminUsername)
      .password(passwordEncoder.encode(adminPassword))
      .authorities(adminRole)
  }

}
