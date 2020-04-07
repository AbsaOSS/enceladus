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

package za.co.absa.enceladus.utils.udf

import za.co.absa.enceladus.utils.error.ErrorMessage

import scala.util.{Failure, Success, Try}

case class UDFResult[T] (
                         result: Option[T],
                         error: Seq[ErrorMessage]
                       )

object UDFResult {
  def success[T](result: Option[T]): UDFResult[T] = {
    UDFResult(result, Seq.empty)
  }

  def fromTry[T](result: Try[Option[T]], columnName: String, rawValue: String, defaultValue: Option[T] = None): UDFResult[T] = {
    result match {
      case Success(success)                       => UDFResult.success(success)
      case Failure(_) if Option(rawValue).isEmpty => UDFResult(defaultValue, Seq(ErrorMessage.stdNullErr(columnName)))
      case Failure(_)                             => UDFResult(defaultValue, Seq(ErrorMessage.stdCastErr(columnName, rawValue)))
    }
  }
}
