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

import org.mockito.{ArgumentMatchersSugar, Mockito}
import org.mockito.MockitoSugar.withObjectMocked
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.dao.UnauthorizedException
import za.co.absa.enceladus.dao.auth.{InvalidMenasCredentials, MenasKerberosCredentials, MenasPlainCredentials}
import za.co.absa.enceladus.dao.rest.RestDaoFactory.MenasSetup

class RestDaoFactorySuite extends AnyWordSpec with Matchers with ArgumentMatchersSugar {

  private val menasApiBaseUrls = List("http://localhost:8080/menas/api")

  "RestDaoFactory::getInstance" should {
    "return a MenasRestDAO instance with a SpnegoAuthClient" when {
      "given a Keytab location" in {
        val keytabCredentials = MenasKerberosCredentials("user", "src/test/resources/user.keytab.example")
        val restDao = RestDaoFactory.getInstance(keytabCredentials, menasApiBaseUrls)
        getAuthClient(restDao.restClient).getClass should be(classOf[SpnegoAuthClient])
      }
    }
    "return a MenasRestDAO instance with a LdapAuthClient" when {
      "given plain MenasCredentials" in {
        val plainCredentials = MenasPlainCredentials("user", "changeme")
        val restDao = RestDaoFactory.getInstance(plainCredentials, menasApiBaseUrls)
        getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
      }
    }
    "throw an error" when {
      "given invalid credentials" in {
        val exception = intercept[UnauthorizedException] {
          RestDaoFactory.getInstance(InvalidMenasCredentials, menasApiBaseUrls)
        }
        exception.getMessage should be("No Menas credentials provided")
      }
    }
    "properly adjusts the starting URL based on the setup type " when {
      val fooCrossHostApiCaller = CrossHostApiCaller(Seq.empty)
      val plainCredentials = MenasPlainCredentials("user", "changeme")
      "when it's round-robin" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Option[Int]], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val restDao = RestDaoFactory.getInstance(plainCredentials, menasApiBaseUrls, None, Option(MenasSetup.RoundRobin))
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            menasApiBaseUrls,
            None,
            None)
        }
      }
      "when it's fallback" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Option[Int]], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val plainCredentials = MenasPlainCredentials("user", "changeme")
          val restDao = RestDaoFactory.getInstance(plainCredentials, menasApiBaseUrls, None, Option(MenasSetup.Fallback))
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            menasApiBaseUrls,
            None,
            Option(0))
        }
      }
      "when the setup type is not specified" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Option[Int]], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val restDao = RestDaoFactory.getInstance(plainCredentials, menasApiBaseUrls, None, None)
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            menasApiBaseUrls,
            None,
            None)
        }
      }
    }
  }

  private def getAuthClient(restClient: RestClient): AuthClient = {
    val field = classOf[RestClient].getDeclaredField("authClient")
    field.setAccessible(true)
    field.get(restClient).asInstanceOf[AuthClient]
  }
}

