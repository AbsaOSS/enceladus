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
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue}

import scala.util.{Failure, Success, Try}

object BinaryFieldValidator extends FieldValidator {
  private def encodingForField(field: TypedStructField): Option[String] =
    field.structField.getMetadataString(MetadataKeys.Encoding)

  private def validateDefaultValueWithGlobal(field: TypedStructField): Seq[ValidationIssue] = {
    tryToValidationIssues(field.defaultValueWithGlobal)
  }

  private def validateExplicitBase64DefaultValue(field: TypedStructField): Seq[ValidationIssue] = {
    val defaultValue: Option[String] = field.structField.getMetadataString(MetadataKeys.DefaultValue)
    val encoding: Option[String] = encodingForField(field)

    (encoding, defaultValue) match {
      case (Some(MetadataValues.Encoding.Base64), Some(encodedValue)) => Try {
        Base64.getDecoder.decode(encodedValue)
      } match {
        case Success(_) => Seq.empty
        case Failure(_) => Seq(ValidationError(s"Invalid default value $encodedValue for Base64 encoding (cannot be decoded)!"))
      }
      case _ => Seq.empty
    }
  }

  private def validateEncoding(field: TypedStructField): Seq[ValidationIssue] = {
    val encoding: Option[String] = encodingForField(field)

    encoding.map(_.toLowerCase) match {
      case Some(MetadataValues.Encoding.Base64) | Some(MetadataValues.Encoding.None) =>
        Seq.empty
      case None => Seq.empty
      case _ => Seq(ValidationError(s"Unsupported encoding for Binary field ${field.structField.name}: '${encoding.get}'"))
    }
  }

  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++ validateDefaultValueWithGlobal(field) ++ validateExplicitBase64DefaultValue(field) ++ validateEncoding(field)
  }
}
