package za.co.absa.enceladus.model.menas

import za.co.absa.enceladus.model.versionedModel.VersionedModel
import scala.reflect.ClassTag

case class AuditFieldName(declaredField: String, humanReadableField: String)

trait Auditable[T <: Product] { self: T =>

  private def fields(implicit ct: ClassTag[T]) = ct.runtimeClass.getDeclaredFields

  private def getFieldIndex(fieldName: String)(implicit ct: ClassTag[T]): Option[Int] = fields.zipWithIndex.find(field => field._1.getName == fieldName).map(_._2)  
  
  def getPrimitiveFieldsAudit(newValue: T, fieldNames: Seq[AuditFieldName])(implicit ct: ClassTag[T]): Seq[AuditTrailChange] = {
    fieldNames.map({ name =>
      val fieldIndex = getFieldIndex(name.declaredField)

      fieldIndex.flatMap({ i =>
          val newVal = newValue.productElement(i)
          val oldVal = self.productElement(i)
          if (newVal != oldVal)
            Some(AuditTrailChange(field = name.declaredField, oldValue = Some(oldVal.toString), newValue = Some(newVal.toString), message = s"${name.humanReadableField} updated."))
          else None
      })

    }).filter(_.isDefined).map(_.get)
  }
  
  def getSeqFieldsAudit(newValue: T, fieldName: AuditFieldName)(implicit ct: ClassTag[T]): Seq[AuditTrailChange] = {
    val index = getFieldIndex(fieldName.declaredField)
    
    index.map({i =>
      val newSeq = newValue.productElement(i).asInstanceOf[Seq[_]]
      val oldSeq = self.productElement(i).asInstanceOf[Seq[_]]
      
      val added = newSeq.diff(oldSeq).map(v => AuditTrailChange(field = fieldName.declaredField, oldValue = None, newValue = Some(v.toString), message = s"${fieldName.humanReadableField} added."))
      val removed = oldSeq.diff(newSeq).map(v => AuditTrailChange(field = fieldName.declaredField, oldValue = Some(v.toString), newValue = None, message = s"${fieldName.humanReadableField} removed."))
      
      removed ++ added
    }).getOrElse(Seq())
  }

  def getAuditMessages(newRecord: T): Seq[AuditTrailChange]
}