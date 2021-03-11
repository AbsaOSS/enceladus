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

package za.co.absa.enceladus.menas.auth.kerberos

import java.util

import org.springframework.ldap.core.DirContextOperations
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.authority.AuthorityUtils
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.ldap.userdetails.LdapAuthoritiesPopulator
import javax.naming.ldap.LdapName

class ActiveDirectoryLdapAuthoritiesPopulator extends LdapAuthoritiesPopulator  {

  import scala.collection.JavaConversions._

  override def getGrantedAuthorities(userData: DirContextOperations, username: String): util.Collection[_ <: GrantedAuthority] = {
    val groups = userData.getStringAttributes("memberOf")

    if (groups == null) {
      AuthorityUtils.NO_AUTHORITIES
    }
    else {
      groups.map({group =>
        val ldapName = new LdapName(group)
        val role = ldapName.getRdn(ldapName.size() - 1).getValue.toString
        new SimpleGrantedAuthority(role)
       }).toList
    }
  }
}
