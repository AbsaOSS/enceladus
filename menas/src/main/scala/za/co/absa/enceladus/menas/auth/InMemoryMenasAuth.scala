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

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.crypto.password.NoOpPasswordEncoder
import org.springframework.stereotype.Component

@Component
class InMemoryMenasAuthentication extends MenasAuthentication {
  @Value("${za.co.absa.enceladus.menas.auth.inmemory.user:}")
  val username: String = ""
  @Value("${za.co.absa.enceladus.menas.auth.inmemory.password:}")
  val password: String = ""

  def validateParams() {
    if (username.isEmpty || password.isEmpty) {
      throw new IllegalArgumentException("Both username and password have to configured for inmemory authentication.")
    }
  }

  override def configure(auth: AuthenticationManagerBuilder) {
    this.validateParams()
    auth
      .inMemoryAuthentication()
      .passwordEncoder(NoOpPasswordEncoder.getInstance())
      .withUser(username)
      .password(password)
      .authorities("ROLE_USER")
  }
}
