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

import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import org.springframework.web.client.{ResourceAccessException, RestClientException}
import za.co.absa.enceladus.dao.rest.CrossHostApiCaller.logger
import za.co.absa.enceladus.dao.{DaoException, RetryableException}

import scala.annotation.tailrec
import scala.util.{Failure, Random, Try}

protected object CrossHostApiCaller {

  private val logger  = LoggerFactory.getLogger(classOf[CrossHostApiCaller])

  private def createInstance(apiBaseUrls: Seq[String],
                             tryCounts: Seq[Int] = Vector.empty,
                             startWith: Option[Int] = None): CrossHostApiCaller = {
    def filterUrls(entry: (String, Int)): Boolean = {
      entry match {
        case ("", _)                          => false
        case (url, tryCount) if tryCount <= 0 =>
          logger.warn(s"Url $url doesn't have a positive try count ($tryCount). Ignoring.")
          false
        case _                                => true
      }
    }

    val urls = apiBaseUrls.toVector
    val tries = tryCounts.toVector
    val triesExtendedToUrlsLength = tries ++ Vector.fill(urls.size - tries.size)(tries.lastOption.getOrElse(1))
    val urlsWithTries = (urls zip triesExtendedToUrlsLength).filter(filterUrls)
    val currentHostIndex = startWith.getOrElse(Random.nextInt(Math.max(urlsWithTries.size, 1)))
    new CrossHostApiCaller(urlsWithTries, currentHostIndex)
  }

  def apply(apiBaseUrls: Seq[String]): CrossHostApiCaller = {
    createInstance(apiBaseUrls)
  }

  def apply(apiBaseUrls: Seq[String], startWith: Int): CrossHostApiCaller = {
    createInstance(apiBaseUrls, Vector.empty, Option(startWith))
  }

  def apply(apiBaseUrls: Seq[String], tryCounts: Seq[Int]): CrossHostApiCaller = {
    createInstance(apiBaseUrls, tryCounts)
  }

  def apply(apiBaseUrls: Seq[String], tryCounts: Seq[Int], startWith: Int): CrossHostApiCaller = {
    createInstance(apiBaseUrls, tryCounts, Option(startWith))
  }

}

protected class CrossHostApiCaller private(private val baseUrlsWithTries: Vector[(String, Int)], private var currentHostIndex: Int)
  extends ApiCaller {

  def baseUrlsCount: Int = baseUrlsWithTries.size

  def currenBasetUrl: (String, Int) = baseUrlsWithTries(currentHostIndex)

  def nextBaseUrl(): (String, Int) = {
    currentHostIndex += 1
    if (currentHostIndex >= baseUrlsCount) {
      currentHostIndex = 0
    }
    currenBasetUrl
  }


  def call[T](fn: String => T): T = {
    def logFailure(error: Throwable, url: String, maxTryCount: Int, attemptNumber: Int, nextUrl: Option[String]): Unit = {
      val rootCause = ExceptionUtils.getRootCauseMessage(error)
      val switching = nextUrl.map(s => s", switching host to $s").getOrElse("")
      logger.warn(s"Request failed on host $url (attemp $attemptNumber of $maxTryCount)$switching - $rootCause")
    }

    @tailrec
    def attempt(url: String, maxTryCount: Int, attemptNumber: Int, urlsTried: Int): Try[T] = {
      val result =Try {
        fn(url)
      }.recoverWith {
        case e @ (_: ResourceAccessException | _: RestClientException) => Failure(DaoException("Server non-responsive", e))
      }
      //using match instead of recoverWith to make the function @tailrec
      result match {
        case Failure(e: RetryableException) if attemptNumber < maxTryCount =>
          logFailure(e, url, maxTryCount, attemptNumber, None)
          attempt(url, maxTryCount, attemptNumber + 1, urlsTried)
        case Failure(e: RetryableException) if urlsTried < baseUrlsCount =>
          val (nextUrl, nextMaxTryCount) = nextBaseUrl()
          logFailure(e, url, maxTryCount, attemptNumber, Option(nextUrl))
          attempt(nextUrl, nextMaxTryCount, 1, urlsTried + 1)
        case _ => result
      }
    }

    attempt(currenBasetUrl._1, currenBasetUrl._2, 1, 1).get
  }

}
