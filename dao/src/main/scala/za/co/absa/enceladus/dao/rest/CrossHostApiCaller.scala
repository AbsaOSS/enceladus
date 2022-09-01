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
import za.co.absa.enceladus.dao.RestApiException
import za.co.absa.enceladus.dao.RetryableException._
import za.co.absa.enceladus.dao.OptionallyRetryableException._

import scala.annotation.tailrec
import scala.util.{Failure, Random, Try}

object CrossHostApiCaller {

  private val logger  = LoggerFactory.getLogger(classOf[CrossHostApiCaller])

  final val DefaultUrlsRetryCount: Int = 0

  private def createInstance(
      apiBaseUrls: Seq[String],
      urlsRetryCount: Int,
      startWith: Option[Int],
      optionallyRetryableExceptions: Set[OptRetryableExceptions],
      retryBackoffStrategy: Int => Unit
  ): CrossHostApiCaller = {
    val maxTryCount: Int = (if (urlsRetryCount < 0) {
      logger.warn(
        s"Urls retry count cannot be negative ($urlsRetryCount). " +
        s"Using default number of retries instead ($DefaultUrlsRetryCount)."
      )
      DefaultUrlsRetryCount
    } else {
      urlsRetryCount
    }) + 1
    val currentHostIndex = startWith.getOrElse(Random.nextInt(Math.max(apiBaseUrls.size, 1)))
    new CrossHostApiCaller(
      apiBaseUrls.toVector, maxTryCount, currentHostIndex, optionallyRetryableExceptions, retryBackoffStrategy
    )
  }

  def apply(
      apiBaseUrls: Seq[String],
      urlsRetryCount: Int = DefaultUrlsRetryCount,
      startWith: Option[Int] = None,
      optionallyRetryableExceptions: Set[OptRetryableExceptions] = Set.empty,
      retryBackoffStrategy: Int => Unit = quadraticRandomizedRetryBackoffStrategy
   ): CrossHostApiCaller = {
    createInstance(apiBaseUrls, urlsRetryCount, startWith, optionallyRetryableExceptions, retryBackoffStrategy)
  }

  private def quadraticRandomizedRetryBackoffStrategy(retryNumber: Int): Unit = {
    val inSeconds: Double = retryNumber * retryNumber + retryNumber * Random.nextDouble()
    val inMillis = inSeconds * 1000
    Thread.sleep(inMillis.toLong)
  }
}

protected class CrossHostApiCaller private(
    apiBaseUrls: Vector[String],
    maxTryCount: Int,
    private var currentHostIndex: Int,
    optionallyRetryableExceptions: Set[OptRetryableExceptions],
    retryBackoffStrategy: Int => Unit
)
  extends ApiCaller {

  private val retryableExceptions: Set[Class[_ <: RestApiException]] = optionallyRetryableExceptions ++
    Set(classOf[DaoException], classOf[AutoRecoverableException])

  def baseUrlsCount: Int = apiBaseUrls.size

  def currentBaseUrl: String = apiBaseUrls(currentHostIndex)

  def nextBaseUrl(): String = {
    currentHostIndex = (currentHostIndex + 1) % baseUrlsCount
    currentBaseUrl
  }


  def call[T](fn: String => T): T = {

    def logFailure(error: Throwable, url: String, attemptNumber: Int, nextUrl: Option[String]): Unit = {
      val rootCause = ExceptionUtils.getRootCauseMessage(error)
      val switching = nextUrl.map(s => s", switching host to $s").getOrElse("")
      logger.warn(s"Request failed on host $url (attempt $attemptNumber of $maxTryCount)$switching - $rootCause")
    }

    @tailrec
    def attempt(url: String, attemptNumber: Int, urlsTried: Int): Try[T] = {
      val result = Try {
        fn(url)
      }.recoverWith {
        case e @ (_: ResourceAccessException | _: RestClientException) =>
          Failure(AutoRecoverableException("Server non-responsive", e))
      }

      //using match instead of recoverWith to make the function @tailrec
      result match {
        case Failure(e: RestApiException) if retryableExceptions.contains(e.getClass) && attemptNumber < maxTryCount =>
          logFailure(e, url, attemptNumber, None)
          retryBackoffStrategy(attemptNumber)
          attempt(url, attemptNumber + 1, urlsTried)

        case Failure(e: RestApiException) if retryableExceptions.contains(e.getClass) && urlsTried < baseUrlsCount =>
          val nextUrl = nextBaseUrl()
          logFailure(e, url, attemptNumber, Option(nextUrl))
          retryBackoffStrategy(1) // to have small delay between trying different URLs
          attempt(nextUrl, 1, urlsTried + 1)

        case _ => result
      }
    }

    attempt(currentBaseUrl,1, 1).get
  }

}
