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

import org.mockito.MockitoSugar.withObjectMocked
import org.mockito.{ArgumentMatchersSugar, Mockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.dao.UnauthorizedException
import za.co.absa.enceladus.dao.auth.{InvalidRestApiCredentials, RestApiKerberosCredentials, RestApiPlainCredentials}
import za.co.absa.enceladus.dao.rest.RestDaoFactory.AvailabilitySetup

class RestDaoFactorySuite extends AnyWordSpec with Matchers with ArgumentMatchersSugar {

  private val restApiBaseUrls = List("http://localhost:8080/rest_api/api")

  "RestDaoFactory::getInstance" should {
    "return a MenasRestDAO instance with a SpnegoAuthClient" when {
      "given a Keytab location" in {
        val keytabCredentials = RestApiKerberosCredentials("user", "src/test/resources/user.keytab.example")
        val restDao = RestDaoFactory.getInstance(keytabCredentials, restApiBaseUrls)
        getAuthClient(restDao.restClient).getClass should be(classOf[SpnegoAuthClient])
      }
    }
    "return a MenasRestDAO instance with a LdapAuthClient" when {
      "given plain RestApiCredentials" in {
        val plainCredentials = RestApiPlainCredentials("user", "changeme")
        val restDao = RestDaoFactory.getInstance(plainCredentials, restApiBaseUrls)
        getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
      }
    }
    "throw an error" when {
      "given invalid credentials" in {
        val exception = intercept[UnauthorizedException] {
          RestDaoFactory.getInstance(InvalidRestApiCredentials, restApiBaseUrls)
        }
        exception.getMessage should be("No REST API credentials provided")
      }
    }
    "properly adjusts the starting URL based on the setup type " when {
      val fooCrossHostApiCaller = CrossHostApiCaller(Seq.empty)
      val plainCredentials = RestApiPlainCredentials("user", "changeme")
      "when it's round-robin" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Int], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val restDao = RestDaoFactory.getInstance(plainCredentials, restApiBaseUrls)
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            restApiBaseUrls,
            CrossHostApiCaller.DefaultUrlsRetryCount,
            None)
        }
      }
      "when it's fallback" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Int], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val plainCredentials = RestApiPlainCredentials("user", "changeme")
          val restDao = RestDaoFactory.getInstance(plainCredentials, restApiBaseUrls, None, AvailabilitySetup.Fallback)
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            restApiBaseUrls,
            CrossHostApiCaller.DefaultUrlsRetryCount,
            Option(0))
        }
      }
      "when the setup type is not specified" in {
        withObjectMocked[CrossHostApiCaller.type] {
          Mockito.when(CrossHostApiCaller.apply(any[Seq[String]], any[Int], any[Option[Int]])).thenReturn(fooCrossHostApiCaller)
          val restDao = RestDaoFactory.getInstance(plainCredentials, restApiBaseUrls)
          getAuthClient(restDao.restClient).getClass should be(classOf[LdapAuthClient])
          Mockito.verify(CrossHostApiCaller, Mockito.times(1)).apply(
            restApiBaseUrls,
            CrossHostApiCaller.DefaultUrlsRetryCount,
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

