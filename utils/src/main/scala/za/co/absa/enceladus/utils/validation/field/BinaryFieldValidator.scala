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

object BinaryFieldValidator extends BinaryFieldValidator

class BinaryFieldValidator extends FieldValidator {
  private def encodingForField(field: TypedStructField): Option[String] =
    field.structField.getMetadataString(MetadataKeys.Encoding)

  private def validateDefaultValue(field: TypedStructField): Seq[ValidationIssue] = {
    tryToValidationIssues(field.defaultValueWithGlobal)
  }

  private def validateExplicitDefaultValue(field: TypedStructField): Seq[ValidationIssue] = {
    val defaultValue = field.structField.getMetadataString(MetadataKeys.DefaultValue)
    val encoding: Option[String] = encodingForField(field)

    (for {
      enc <- encoding if enc == MetadataValues.Encoding.Base64
      value <- defaultValue
    } yield {
      // only base64 and explicit default value could result in validation issues here:
      Try {
        Base64.getDecoder.decode(value)
      } match {
        case Success(_) => Seq.empty
        case Failure(_) => Seq(ValidationError(s"Invalid default value $value for Base64 encoding (cannot be decoded)!"))
      }
    }).getOrElse(Seq.empty)
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
    super.validate(field) ++ validateDefaultValue(field) ++ validateExplicitDefaultValue(field) ++ validateEncoding(field)
  }
}
