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

import org.springframework.security.ldap.DefaultSpringSecurityContextSource
import javax.security.auth.Subject
import java.util.Hashtable
import javax.naming.directory.DirContext
import javax.naming.Context
import java.security.PrivilegedAction

class MenasKerberosLdapContextSource(url: String, subject: Subject) extends DefaultSpringSecurityContextSource(url) {
  override def getDirContextInstance(environment: Hashtable[String, Object]): DirContext = {
    import scala.collection.JavaConversions._
    
    environment.put(Context.SECURITY_AUTHENTICATION, "GSSAPI")
    val sup = super.getDirContextInstance _

    logger.debug(s"Trying to authenticate to LDAP as ${subject.getPrincipals}")
    Subject.doAs(subject, new PrivilegedAction[DirContext]() {
      override def run(): DirContext = {
        sup(environment)
      }
    })
  }
}