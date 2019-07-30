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

package za.co.absa.enceladus.model.menas.audit

import za.co.absa.enceladus.model.versionedModel.VersionedModel
import scala.reflect.ClassTag

/**
 * Trait for all auditable Menas entities
 */
trait Auditable[T <: Product] { self: T =>

  /**
   * Get an array of all fields defined in T
   */
  private def fields(implicit ct: ClassTag[T]) = ct.runtimeClass.getDeclaredFields

  val createdMessage: AuditTrailEntry

  /**
   * Get an index of a given field in T
   */
  private def getFieldIndex(fieldName: String)(implicit ct: ClassTag[T]): Option[Int] = fields.zipWithIndex.find(field => field._1.getName == fieldName).map(_._2)

  /**
   * Get list of audit entries for specified fields of primitive types
   *
   * @param newValue New record of type T
   * @param fieldNames List of fields for which the audit messages are to be generated and their human readable names
   */
  private[model] def getPrimitiveFieldsAudit(newValue: T, fieldNames: Seq[AuditFieldName])(implicit ct: ClassTag[T]): Seq[AuditTrailChange] = {
    fieldNames.map({ name =>
      val fieldIndex = getFieldIndex(name.declaredField)

      fieldIndex.flatMap({ i =>
          val newVal = newValue.productElement(i)
          val oldVal = self.productElement(i)
          if (newVal != oldVal) {
            Some(AuditTrailChange(field = name.declaredField, oldValue = Some(unwrapOption(oldVal).toString), newValue = Some(unwrapOption(newVal).toString), message = s"${name.humanReadableField} updated."))
          } else {
            None
          }
      })
    }).collect {
      case Some(change) => change
    }
  }

  /**
   * Get list of audit messages for a specific Seq field.
   *
   * @param newValue Newer record of type T
   * @param fieldName Name of the field to generate the audit messages for
   */
  private[model] def getSeqFieldsAudit(newValue: T, fieldName: AuditFieldName)(implicit ct: ClassTag[T]): Seq[AuditTrailChange] = {
    val index = getFieldIndex(fieldName.declaredField)

    index.map({i =>
      val newSeq = newValue.productElement(i).asInstanceOf[Seq[_]]
      val oldSeq = self.productElement(i).asInstanceOf[Seq[_]]

      val added = newSeq.diff(oldSeq).map(v => AuditTrailChange(field = fieldName.declaredField, oldValue = None, newValue = Some(unwrapOption(v).toString), message = s"${fieldName.humanReadableField} added."))
      val removed = oldSeq.diff(newSeq).map(v => AuditTrailChange(field = fieldName.declaredField, oldValue = Some(unwrapOption(v).toString), newValue = None, message = s"${fieldName.humanReadableField} removed."))

      removed ++ added
    }).getOrElse(Seq())
  }

  /**
   * Utility function to unwrap defined option values (if Some) or keep the original value otherwise.
   */
  private def unwrapOption(obj: Any): Any = obj match {
    case Some(x) => x
    case other => other
  }

  def getAuditMessages(newRecord: T): AuditTrailEntry
}
