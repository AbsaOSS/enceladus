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

import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.security.core.{Authentication, GrantedAuthority}
import org.springframework.stereotype.Component

@Component("authConstants")
class AuthConstants @Autowired()() {
  @Value("${menas.auth.admin.role}")
  val AdminRole: String = ""

  def hasAdminRole(auth: Authentication): Boolean = {
    auth.getAuthorities.toArray(Array[GrantedAuthority]()).map(auth => auth.getAuthority).contains(AdminRole)
  }
}

object AuthConstants {
  val JwtCookieKey: String = "JWT"
  val CsrfTokenKey: String = "X-CSRF-TOKEN"
  val RolesKey: String = "Roles"

  @Value("${menas.auth.roles.regex:}")
  val RolesRegex: String = ""

  def filterByRolesRegex(roles: Seq[String]): Seq[String] = {
    if (RolesRegex.isEmpty) { roles }
    else { roles.filter(authority => authority.matches(RolesRegex)) }
  }
}
