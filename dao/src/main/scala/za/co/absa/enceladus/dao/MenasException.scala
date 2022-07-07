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

package za.co.absa.enceladus.dao

abstract class MenasException(message: String, cause: Throwable) extends Exception(message, cause)

abstract class RetryableException(message: String, cause: Throwable) extends MenasException(message, cause)

object RetryableException {

  type RetryableExceptions = Class[_ <: RetryableException]

  final case class DaoException(private val message: String,
                                private val cause: Throwable = None.orNull)
    extends RetryableException(message, cause)

  final case class AutoRecoverableException(private val message: String,
                                            private val cause: Throwable = None.orNull)
    extends RetryableException(message, cause)
}

abstract class OptionallyRetryableException(message: String, cause: Throwable) extends MenasException(message, cause)

object OptionallyRetryableException {

  type OptRetryableExceptions = Class[_ <: OptionallyRetryableException]

  final case class UnauthorizedException(private val message: String,
                                         private val cause: Throwable = None.orNull)
    extends OptionallyRetryableException(message, cause)

  final case class NotFoundException(private val message: String,
                                     private val cause: Throwable = None.orNull)
    extends OptionallyRetryableException(message, cause)

  val mapIntToOptionallyRetryableException: Map[Int, OptRetryableExceptions] = Map(
    401 -> classOf[UnauthorizedException],
    403 -> classOf[UnauthorizedException],
    404 -> classOf[NotFoundException]
  )
}
