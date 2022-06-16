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

import org.springframework.http.HttpStatus
import za.co.absa.enceladus.dao.{CustomException, DaoException}


object MenasRetryableExceptionsStringParser {

  private val exceptionRegex = """^([1-5]\d{2})$""".r

  def parse(exceptionsString: String): Set[CustomException] = {
    exceptionsString
      .trim
      .split(";")
      .flatMap(expandExceptions)
      .toSet
  }

  private def expandExceptions(singleExceptionString: String): Option[CustomException] = {
    singleExceptionString match {
      case exceptionRegex(exception) =>
        val currStatus = HttpStatus.valueOf(exception.toInt)
        val currException = new CustomException(s"Optionally retryable exception - $currStatus", None.orNull) {}
        Some(currException)
      case "" =>
        None
      case _ =>
        throw DaoException("Malformed Menas retryableExceptions string")
    }
  }
}
