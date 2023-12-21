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
import za.co.absa.enceladus.dao.{NotRetryableException, OptionallyRetryableException, RetryableException}

class CrossHostApiCallerSuite extends BaseTestSuite {

  private val restClient = mock[RestClient]

  private def zeroWaitRetryBackoffStrategy(retryNumber: Int): Int = 0

  before {
    Mockito.reset(restClient)
  }

  "CrossHostApiCaller" should {
    "cycle through urls" in {
      val crossHostApiCaller = CrossHostApiCaller(
        Vector("a", "b", "c", "d"), DefaultUrlsRetryCount,  startWith = Some(1))
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

        val result = CrossHostApiCaller(
          Vector("a", "b", "c"),
          DefaultUrlsRetryCount,
          startWith = Some(0),
          retryBackoffStrategy = zeroWaitRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.only()).sendGet[String]("a")
      }

      "only some calls fail with a retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a"))
          .thenThrow(RetryableException.DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(RetryableException.DaoException("Something went wrong B"))
          .thenReturn("success")

        val result = CrossHostApiCaller(
          Vector("a", "b", "c"),
          2,
          Some(0),
          retryBackoffStrategy = zeroWaitRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(3)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.never()).sendGet[String]("c")
      }

      "despite retry count is negative" in {
        Mockito.when(restClient.sendGet[String]("a"))
          .thenThrow(RetryableException.DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(RetryableException.DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c")).thenReturn("success")

        val result = CrossHostApiCaller(
          Vector("a", "b", "c"),
          -2,
          Some(0),
          retryBackoffStrategy = zeroWaitRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("c")
      }

      "optionallyRetryable notFound exception is retried" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(
          OptionallyRetryableException.NotFoundException("Something went wrong A")
        )
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(RetryableException.DaoException("Something went wrong B"))
          .thenReturn("success")

        val result = CrossHostApiCaller(
          Vector("a", "b", "c"),
          2,
          Some(0),
          Set(classOf[OptionallyRetryableException.NotFoundException]),
          retryBackoffStrategy = zeroWaitRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(3)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.never()).sendGet[String]("c")
      }

      "optionallyRetryable unauthorized exception is retried twice" in {
        Mockito.when(restClient.sendGet[String]("a"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(OptionallyRetryableException.ForbiddenException("Something went wrong B"))
          .thenThrow(OptionallyRetryableException.ForbiddenException("Something went wrong B"))
          .thenReturn("success")

        val result = CrossHostApiCaller(
          Vector("a", "b", "c"),
          2,
          Some(0),
          Set(classOf[OptionallyRetryableException.ForbiddenException]),
          retryBackoffStrategy = zeroWaitRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        result should be("success")
        Mockito.verify(restClient, Mockito.times(3)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(3)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.never()).sendGet[String]("c")
      }

      "retryBackoffStrategy is called with retry number increasing by 1 in each attempt on single URI retrying" in {
        val mockedRetryBackoffStrategy = mock[Int => Int]
        Mockito
          .when(restClient.sendGet[String]("a"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
          .thenReturn("success")

        CrossHostApiCaller(
          Vector("a"),
          3,
          Some(0),
          Set(classOf[OptionallyRetryableException.ForbiddenException]),
          retryBackoffStrategy = mockedRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        val inOrder = Mockito.inOrder(mockedRetryBackoffStrategy)
        inOrder.verify(mockedRetryBackoffStrategy).apply(1)
        inOrder.verify(mockedRetryBackoffStrategy).apply(2)
        inOrder.verify(mockedRetryBackoffStrategy).apply(3)
        inOrder.verify(mockedRetryBackoffStrategy, Mockito.never()).apply(4)
      }

      "retryBackoffStrategy is called with retry number = 1 when switching URIs" in {
        val mockedRetryBackoffStrategy = mock[Int => Int]
        Mockito
          .when(restClient.sendGet[String]("a"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
        Mockito
          .when(restClient.sendGet[String]("b"))
          .thenReturn("success")

        CrossHostApiCaller(
          Vector("a", "b"),
          0,
          Some(0),
          Set(classOf[OptionallyRetryableException.ForbiddenException]),
          retryBackoffStrategy = mockedRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        val inOrder = Mockito.inOrder(mockedRetryBackoffStrategy)
        inOrder.verify(mockedRetryBackoffStrategy).apply(1)
        inOrder.verify(mockedRetryBackoffStrategy, Mockito.never()).apply(2)
      }

      "retryBackoffStrategy is called with retry number starting again from 1 when using next URI" in {
        val mockedRetryBackoffStrategy = mock[Int => Int]
        Mockito
          .when(restClient.sendGet[String]("a"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
          .thenThrow(new ResourceAccessException("Something went wrong A"))
        Mockito
          .when(restClient.sendGet[String]("b"))
          .thenThrow(new ResourceAccessException("Something went wrong B"))
          .thenThrow(new ResourceAccessException("Something went wrong B"))
          .thenReturn("success")

        CrossHostApiCaller(
          Vector("a", "b"),
          2,
          Some(0),
          Set(classOf[OptionallyRetryableException.ForbiddenException]),
          retryBackoffStrategy = mockedRetryBackoffStrategy
        ).call { str =>
          restClient.sendGet[String](str)
        }

        val inOrder = Mockito.inOrder(mockedRetryBackoffStrategy)
        inOrder.verify(mockedRetryBackoffStrategy).apply(1)
        inOrder.verify(mockedRetryBackoffStrategy).apply(2)
        inOrder.verify(mockedRetryBackoffStrategy, Mockito.never()).apply(3)
        // times 2 because one time in between URIs switch, and another after switched to new URI and failed
        inOrder.verify(mockedRetryBackoffStrategy, Mockito.times(2)).apply(1)
        inOrder.verify(mockedRetryBackoffStrategy).apply(2)
        inOrder.verify(mockedRetryBackoffStrategy, Mockito.never()).apply(3)
      }
    }

    "propagate the exception" when {
      "all calls fail with a retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a"))
          .thenThrow(RetryableException.DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(RetryableException.DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c"))
          .thenThrow(RetryableException.DaoException("Something went wrong C"))

        val exception = intercept[RetryableException.DaoException] {
          CrossHostApiCaller(
            Vector("a", "b", "c"),
            0,
            Some(0),
            retryBackoffStrategy = zeroWaitRetryBackoffStrategy
          ).call { str =>
            restClient.sendGet[String](str)
          }
        }

        exception.getMessage should be("Something went wrong C")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(1)).sendGet[String]("c")
      }

      "all calls fail with a retryable exception over multiple attempts" in {
        Mockito.when(restClient.sendGet[String]("a"))
          .thenThrow(RetryableException.DaoException("Something went wrong A"))
        Mockito.when(restClient.sendGet[String]("b"))
          .thenThrow(RetryableException.DaoException("Something went wrong B"))
        Mockito.when(restClient.sendGet[String]("c"))
          .thenThrow(RetryableException.DaoException("Something went wrong C"))

        val exception = intercept[RetryableException.DaoException] {
          CrossHostApiCaller(
            Vector("a", "b", "c"),
            1,
            Some(0),
            retryBackoffStrategy = zeroWaitRetryBackoffStrategy
          ).call { str =>
            restClient.sendGet[String](str)
          }
        }

        exception.getMessage should be("Something went wrong C")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("a")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("b")
        Mockito.verify(restClient, Mockito.times(2)).sendGet[String]("c")
      }

      "any call fails with a non-retryable exception" in {
        Mockito.when(restClient.sendGet[String]("a")).thenThrow(
          new ResourceAccessException("Something went wrong A")
        )
        Mockito.when(restClient.sendGet[String]("b")).thenThrow(
          NotRetryableException.AuthenticationException("Wrong credentials")
        )

        val exception = intercept[NotRetryableException.AuthenticationException] {
          CrossHostApiCaller(
            Vector("a", "b", "c"),
            0,
            Some(0),
            retryBackoffStrategy = zeroWaitRetryBackoffStrategy
          ).call { str =>
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

  "CrossHostApiCaller::quadraticRandomizedRetryBackoffStrategy" should {
    "return number of milliseconds that are in range bounded by 1000 * (x^2 + x*random_0_to_1)" in {
      val when0 = CrossHostApiCaller.quadraticRandomizedRetryBackoffStrategy(0)
      when0 shouldEqual 0

      val when1 = CrossHostApiCaller.quadraticRandomizedRetryBackoffStrategy(1)
      when1 should be >= 1000
      when1 should be <= 2000

      val when5 = CrossHostApiCaller.quadraticRandomizedRetryBackoffStrategy(5)
      when5 should be >= 25000
      when5 should be <= 30000
    }
  }

}
