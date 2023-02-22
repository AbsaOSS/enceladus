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

package za.co.absa.enceladus.model

import com.fasterxml.jackson.annotation.JsonIgnore
import za.co.absa.enceladus.model.Validation.ValidationRecord
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema => AosSchema}

import scala.annotation.meta.field
import scala.beans.BeanProperty

object Validation {

  type ValidationRecord = Map[String, List[String]]

  val NotSpecified = "not specified"

  val empty: Validation = Validation()

  def merge(a: Validation, b: Validation) : Validation = a.merge(b)
}

case class Validation (
  @(AosSchema@field)(implementation = classOf[java.util.Map[String, Array[String]]],
    example = "{\"field1\": [\"error1\"]}")
  @BeanProperty errors: ValidationRecord = Map(),
  @(AosSchema@field)(implementation = classOf[java.util.Map[String, Array[String]]],
    example = "{\"field2\": [\"warning1\", \"warning2\"]}")
  @BeanProperty warnings: ValidationRecord = Map()
) {

  @JsonIgnore
  def isValid: Boolean = errors.isEmpty

  @JsonIgnore
  def isEmpty: Boolean = errors.isEmpty && warnings.isEmpty

  @JsonIgnore
  def nonEmpty: Boolean = errors.nonEmpty || warnings.nonEmpty

  def withError(key: String, error: String): Validation = {
    this.copy(errors = errors + (key -> (error :: errors.getOrElse(key, Nil))))
  }

  def withWarning(key: String, warning: String): Validation = {
    this.copy(warnings = warnings + (key -> (warning :: warnings.getOrElse(key, Nil))))
  }

  def withErrorIf(condition: Boolean, key: => String, error: => String): Validation = {
    if (condition) withError(key, error) else this
  }

  def withWarningIf(condition: Boolean, key: => String, warning: => String): Validation = {
    if (condition) withWarning(key, warning) else this
  }

  def merge(validation: Validation): Validation = {
    def mergeKeyedMaps(first: ValidationRecord,
                       second: ValidationRecord): ValidationRecord = {
      first.foldLeft(second) { case (acc, (key, list)) =>
        acc + (key -> (acc.getOrElse(key, List.empty[String]) ++ list))
      }
    }

    val mergedErrMaps = mergeKeyedMaps(validation.errors, errors)
    val mergedWarnMaps = mergeKeyedMaps(validation.warnings, warnings)

    Validation(mergedErrMaps, mergedWarnMaps)
  }
}
