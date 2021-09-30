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

package za.co.absa.enceladus.dao.rest

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.dao.UnauthorizedException
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentials, MenasKerberosCredentials, MenasPlainCredentials}

class RestDaoFactorySuite extends AnyWordSpec with Matchers {

  private val menasApiBaseUrls = List("http://localhost:8080/menas/api")
  private val menasUrlsTryCounts = List(1)

  "RestDaoFactory::getInstance" should {
    "return a MenasRestDAO instance with a SpnegoAuthClient" when {
      "given a Keytab location" in {
        val keytabCredentials = MenasKerberosCredentials("user", "src/test/resources/user.keytab.example")
        val restDao = RestDaoFactory.getInstance(keytabCredentials, menasApiBaseUrls, menasUrlsTryCounts)
        getAuthClient(restDao.restClient).getClass should be(classOf[SpnegoAuthClient])
      }
    }
    "return a MenasRestDAO instance with a LdapAuthClient" when {
      "given plain MenasCredentials" in {
        val plainCredentials = MenasPlainCredentials("user", "changeme")
        val restDao = RestDaoFactory.getInstance(plainCredentials, menasApiBaseUrls, menasUrlsTryCounts)
        getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
      }
    }
    "throw an error" when {
      "given invalid credentials" in {
        val exception = intercept[UnauthorizedException] {
          RestDaoFactory.getInstance(InvalidMenasCredentials, menasApiBaseUrls, menasUrlsTryCounts)
        }
        exception.getMessage should be("No Menas credentials provided")
      }
    }
  }

  private def getAuthClient(restClient: RestClient): AuthClient = {
    val field = classOf[RestClient].getDeclaredField("authClient")
    field.setAccessible(true)
    field.get(restClient).asInstanceOf[AuthClient]
  }

}
