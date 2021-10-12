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

import org.mockito.Mockito
import org.springframework.web.client.ResourceAccessException
import za.co.absa.enceladus.dao.rest.CrossHostApiCaller.DefaultUrlsRetryCount
import za.co.absa.enceladus.dao.{DaoException, UnauthorizedException}

class CrossHostApiCallerSuite extends BaseTestSuite {

  private val restClient = mock[RestClient]

  before {
    Mockito.reset(restClient)
  }

  "CrossHostApiCaller" should {
    "cycle through urls" in {
      val crossHostApiCaller = CrossHostApiCaller(Vector("a", "b", "c", "d"), DefaultUrlsRetryCount,  startWith = Some(1))
      crossHostApiCaller.nextBaseUrl() should be("c")
      crossHostApiCaller.nextBaseUrl() should be("d")
      crossHostApiCaller.nextBaseUrl() should be("a")
      crossHostApiCaller.nextBaseUrl() should be("b")
      crossHostApiCaller.nextBaseUrl() should be("c")
    }
  }

  "CrossHostApiCaller::call" should {
    "return the result of the first successful call" when {
      "there are no failures" in {
        Mockito.when(restClient.sendGet[String]("a")).thenReturn("success")

        val result = CrossHostApiCaller(Vector("a", "b", "c"), DefaultUrlsRetryCount, startWith = Some(0)).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.only()).sendGet[String]("a")
      }

      "only some calls fail with a retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(DaoException("Something went wrong B"))
          .thenReturn("success")

        val result = CrossHostApiCaller(Vector("a", "b", "c"), 2, Some(0)).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(3)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.never()).sendGet[String]("c")
      }

      "despite retry count is negative" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b")).thenThrow(DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c")).thenReturn("success")

        val result = CrossHostApiCaller(Vector("a", "b", "c"), -2, Some(0)).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("c")
      }
    }

    "propagate the exception" when {
      "all calls fail with a retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b")).thenThrow(DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c")).thenThrow(DaoException("Something went wrong C"))

        val exception = intercept[DaoException] {
          CrossHostApiCaller(Vector("a", "b", "c"), 0, Some(0)).call { str =>
            restClient.sendGet[String](str)
          }
        }

        exception.getMessage should be("Something went wrong C")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("c")
      }

      "all calls fail with a retryable exception over multiple attempts" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b")).thenThrow(DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c")).thenThrow(DaoException("Something went wrong C"))

        val exception = intercept[DaoException] {
          CrossHostApiCaller(Vector("a", "b", "c"), 1, Some(0)).call { str =>
            restClient.sendGet[String](str)
          }
        }

        exception.getMessage should be("Something went wrong C")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("c")
      }

      "any call fails with a non-retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(new ResourceAccessException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b")).thenThrow(UnauthorizedException("Wrong credentials"))

        val exception = intercept[UnauthorizedException] {
          CrossHostApiCaller(Vector("a", "b", "c"), 0, Some(0)).call { str =>
            restClient.sendGet[String](str)
          }
        }

        exception.getMessage should be("Wrong credentials")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.never()).sendGet[String]("c")
      }
    }

    "fail on not having Urls" when {
      "none are provided" in {
        val exception = intercept[IndexOutOfBoundsException] {
          CrossHostApiCaller(Vector()).call { str =>
            restClient.sendGet[String](str)
          }
        }
        exception.getMessage should be ("0")
      }
    }
  }

}
