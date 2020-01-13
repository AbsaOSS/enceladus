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

package za.co.absa.enceladus.utils.validation.field

import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.validation.ValidationIssue

import scala.util.Try


/**
  * Scalar types schema validation against default value
  */
object ScalarFieldValidator extends ScalarFieldValidator

class ScalarFieldValidator extends FieldValidator {

  private def validateDefaultValue(field: TypedStructField): Try[Any] = {
    field.defaultValueWithGlobal
  }

  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++ tryToValidationIssues(validateDefaultValue(field))
  }
}
