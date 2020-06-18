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

import java.util.Base64

import za.co.absa.enceladus.utils.implicits.StructFieldImplicits._
import za.co.absa.enceladus.utils.schema.{MetadataKeys, MetadataValues}
import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.types.TypedStructField.BinaryTypeStructField
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue, ValidationWarning}

import scala.util.{Failure, Success, Try}

object BinaryFieldValidator extends FieldValidator {

  private def validateDefaultValueWithGlobal(field: BinaryTypeStructField): Seq[ValidationIssue] = {
    tryToValidationIssues(field.defaultValueWithGlobal)
  }

  private def validateExplicitBase64DefaultValue(field: BinaryTypeStructField): Seq[ValidationIssue] = {
    val defaultValue: Option[String] = field.structField.getMetadataString(MetadataKeys.DefaultValue)

    (field.normalizedEncoding, defaultValue) match {
      case (None, Some(encodedDefault)) =>
        Seq(ValidationWarning(s"Default value of '$encodedDefault' found, but no encoding is specified. Assuming 'none'."))
      case (Some(MetadataValues.Encoding.Base64), Some(encodedValue)) =>
        Try {
          Base64.getDecoder.decode(encodedValue)
        } match {
          case Success(_) => Seq.empty
          case Failure(_) => Seq(ValidationError(s"Invalid default value $encodedValue for Base64 encoding (cannot be decoded)!"))
        }
      case _ => Seq.empty
    }
  }

  private def validateEncoding(field: BinaryTypeStructField): Seq[ValidationIssue] = {
    field.normalizedEncoding match {
      case Some(MetadataValues.Encoding.Base64) | Some(MetadataValues.Encoding.None) =>
        Seq.empty
      case None => Seq.empty
      case _ => Seq(ValidationError(s"Unsupported encoding for Binary field ${field.structField.name}: '${field.normalizedEncoding.get}'"))
    }
  }

  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++ (
      field match {
        case bField: BinaryTypeStructField =>
          validateDefaultValueWithGlobal(bField) ++ validateExplicitBase64DefaultValue(bField) ++ validateEncoding(bField)
        case _ => Seq(ValidationError("BinaryFieldValidator can validate only fields of type BinaryTypeStructField"))
      })
  }
}
