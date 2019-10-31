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

package za.co.absa.enceladus.dao.rest

import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import org.springframework.web.client.ResourceAccessException
import za.co.absa.enceladus.dao.{DaoException, RetryableException}

import scala.util.{Failure, Random, Try}

protected object CrossHostApiCaller {

  def apply(apiBaseUrls: List[String]): CrossHostApiCaller = {
    new CrossHostApiCaller(apiBaseUrls, Random.nextInt(apiBaseUrls.size))
  }

}

protected class CrossHostApiCaller(apiBaseUrls: List[String], var currentHostIndex: Int) extends ApiCaller {
  private val logger  = LoggerFactory.getLogger(this.getClass)

  private val maxAttempts = apiBaseUrls.size - 1

  def call[T](fn: String => T): T = {

    def attempt(index: Int, attemptCount: Int = 0): Try[T] = {
      currentHostIndex = index
      val currentBaseUrl = apiBaseUrls(index)
      Try {
        fn(currentBaseUrl)
      }.recoverWith {
        case e: ResourceAccessException => Failure(DaoException("Server non-responsive", e))
      }.recoverWith {
        case e: RetryableException if attemptCount < maxAttempts =>
          val nextIndex = (index + 1) % apiBaseUrls.size
          val nextBaseUrl = apiBaseUrls(nextIndex)
          val rootCause = ExceptionUtils.getRootCauseMessage(e)
          logger.warn(s"Request failed on host $currentBaseUrl, switching host to $nextBaseUrl - $rootCause")
          attempt(nextIndex, attemptCount + 1)
      }
    }

    attempt(currentHostIndex).get
  }

}
