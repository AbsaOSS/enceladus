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

package za.co.absa.enceladus.utils.validation.field
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue}

object FractionalFieldValidator extends FractionalFieldValidator

class FractionalFieldValidator extends ScalarFieldValidator {
  private def validateAllowInfinity(field: TypedStructField): Seq[ValidationIssue] = {
    if (field.hasMetadataKey(MetadataKeys.allowInfinity) && field.getMetadataStringAsBoolean(MetadataKeys.allowInfinity).isEmpty) {
      Seq(ValidationError(s"allowInfinity metadata value of field '${field.name}' is not Boolean in String format"))
    } else {
      Nil
    }
  }

  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    validateAllowInfinity(field) ++ super.validate(field)
  }
}

