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

package za.co.absa.enceladus.menas.auth

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.authentication.configurers.provisioning.InMemoryUserDetailsManagerConfigurer
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.stereotype.Component

@Component
class InMemoryMenasAuthentication extends MenasAuthentication {
  @Value("${menas.auth.inmemory.user:}")
  private val username: String = ""
  @Value("${menas.auth.inmemory.password:}")
  private val password: String = ""

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
  }

}
