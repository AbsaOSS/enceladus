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

package za.co.absa.enceladus.rest_api.controllers.v3

import za.co.absa.enceladus.rest_api.utils.implicits._

import java.util.Optional
import scala.util.{Failure, Success, Try}

object ControllerPagination {
  val DefaultOffset: Int = 0
  val DefaultLimit: Int = 20

  /**
   * Offset value is extracted from `optField`, otherwise `defaultValue` returned
   *
   * @param optField
   * @param defaultValue when extraction fails, this value is used. Default = [[ControllerPagination#DefaultOffset()]]
   * @return
   */
  def extractOptionalOffsetOrDefault(optField: Optional[String], defaultValue: Int = DefaultOffset): Int = {
    extractOptionOffsetOrDefault(optField.toScalaOption, defaultValue)
  }

  /** Offset value is extracted from `optField`, otherwise `defaultValue` returned
   *
   * @param optField
   * @param defaultValue when extraction fails, this value is used. Default = [[ControllerPagination#DefaultOffset()]]
   * @return
   */
  def extractOptionOffsetOrDefault(optField: Option[String], defaultValue: Int = DefaultOffset): Int = {
    extractDefinedValueOrDefault(optField, defaultValue)
  }

  /**
   * Limit value is extracted from `optField`, otherwise `defaultValue` returned
   *
   * @param optField
   * @param defaultValue when extraction fails, this value is used. Default = [[ControllerPagination#DefaultLimit()]]
   * @return
   */
  def extractOptionalLimitOrDefault(optField: Optional[String], defaultValue: Int = DefaultLimit): Int = {
    extractOptionLimitOrDefault(optField.toScalaOption, defaultValue)
  }

  /**
   * Limit value is extracted from `optField`, otherwise `defaultValue` returned
   *
   * @param optField
   * @param defaultValue when extraction fails, this value is used. Default = [[ControllerPagination#DefaultLimit()]]
   * @return
   */
  def extractOptionLimitOrDefault(optField: Option[String], defaultValue: Int = DefaultLimit): Int = {
    extractDefinedValueOrDefault(optField, defaultValue)
  }

  /**
   * For the `optField` we try to extract int value
   *
   * @param optField     value to attempt to extract from
   * @param defaultValue value to use if extraction fails
   * @return On extraction success, `extractedIntValue` is returned, otherwise (empty or invalid) `defaultValue` is returned.
   */
  private def extractDefinedValueOrDefault(optField: Option[String], defaultValue: Int): Int = {
    optField match {
      case None => defaultValue
      case Some(intAsString) => Try(intAsString.toInt) match {
        case Success(value) => value
        case Failure(_) => defaultValue
      }
    }
  }

}
