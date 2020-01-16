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

import org.springframework.security.ldap.search.FilterBasedLdapUserSearch
import org.springframework.ldap.core.support.BaseLdapPathContextSource
import org.springframework.ldap.core.DirContextOperations

class KerberosLdapUserSearch(searchBase: String, searchFilter: String, contextSource: BaseLdapPathContextSource)
  extends FilterBasedLdapUserSearch(searchBase, searchFilter, contextSource) {

  override def searchForUser(username: String): DirContextOperations = {
    val user = if(username.contains("@")) {
      username.split("@").head
    } else {
      username
    }
    super.searchForUser(user)
  }

}
