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

package test

import za.co.absa.enceladus.dao.EnceladusRestDAO
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.BasicConfigurator
import java.net.URI
import javax.security.auth.kerberos.DelegationPermission
import org.springframework.security.kerberos.client.KerberosRestTemplate
import org.apache.hadoop.security.UserGroupInformation
import sun.security.krb5.internal.ktab._

object Test extends App {

  import scala.collection.JavaConversions._

  BasicConfigurator.configure();
  Logger.getRootLogger().setLevel(Level.DEBUG)
  System.setProperty("javax.net.debug", "true")
  System.setProperty("sun.security.krb5.debug", "true")
  //  System.setProperty("java.security.auth.login.config", "login.conf")

  //  System.setProperty("java.security.krb5.realm", "CORP.DSARENA.COM")
  //    System.setProperty("java.security.krb5.kdc", "corp.dsarena.com")
  System.setProperty("java.security.krb5.conf", "/tmp/krb5purecorp.conf")

  val keytab = "/tmp/SVC-dedceadquery.keytab"

//  val keytab = KeyTab.getInstance(new java.io.File("/tmp/SVC-dedceadquery.keytab"))
//  println(keytab.getOneName.getName)
  
    EnceladusRestDAO.spnegoLogin(keytab)

  //  println(EnceladusRestDAO.getDataset("TestDS", 1, menasKerberosLogin.principal, menasKerberosLogin.keytabLocation))

  //  val client = new KerberosRestTemplate(menasKerberosLogin.keytabLocation, menasKerberosLogin.principal)
  //  val client = new CustomKerberosRestTemplate(menasKerberosLogin.keytabLocation, menasKerberosLogin.principal)
  //
  //  try {
  //    val resp = client.getForEntity(new URI("http://zausdcrapp0064.corp.dsarena.com/menas/api/spnego/login"), classOf[String])
  //
  //    println(resp.getStatusCode)
  //    println(resp.getBody)
  //    resp.getHeaders.foreach(println _)
  //  } catch {
  //    case e => e.printStackTrace()
  //  }
//  EnceladusRestDAO.spnegoLogin(menasKerberosLogin.principal, menasKerberosLogin.keytabLocation)
//  EnceladusRestDAO.getSchema("TestSchema", 2)
}