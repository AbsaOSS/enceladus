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

package za.co.absa.enceladus.dao

import org.scalatest.{Matchers, WordSpec}
import za.co.absa.enceladus.dao.menasplugin.{MenasPlainCredentials, MenasKerberosCredentials}

class RestDaoFactorySuite extends WordSpec with Matchers {

  "RestDaoFactory::getInstance" should {
    "return a MenasRestDAO instance with a SpnegoAuthClient" when {
      "given a Keytab location" in {
        val keytabCredentials = MenasKerberosCredentials("user", "src/test/resources/user.keytab.example")
        val restDao = RestDaoFactory.getInstance(keytabCredentials)
        restDao.apiBaseUrl should be("http://localhost:8080/menas/api")
        restDao.authClient.getClass should be(classOf[SpnegoAuthClient])
      }
    }
    "return a MenasRestDAO instance with a LdapAuthClient" when {
      "given plain MenasCredentials" in {
        val plainCredentials = MenasPlainCredentials("user", "changeme")
        val restDao = RestDaoFactory.getInstance(plainCredentials)
        restDao.apiBaseUrl should be("http://localhost:8080/menas/api")
        restDao.authClient.getClass should be(classOf[LdapAuthClient])
      }
    }
  }

}
