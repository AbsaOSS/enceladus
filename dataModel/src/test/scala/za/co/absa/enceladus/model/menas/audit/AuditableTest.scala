package za.co.absa.enceladus.model.menas.audit

import org.scalatest.FunSuite
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.{DropConformanceRule, LiteralConformanceRule}

class AuditableTest extends FunSuite {
  val obj1 = Dataset(name = "Test DS",
    version = 0,
    hdfsPath = "oldPath",
    hdfsPublishPath = "oldPublishPath",
    schemaName = "someSchema",
    schemaVersion = 0,
    conformance = List(DropConformanceRule(order = 0, controlCheckpoint = true, outputColumn = "toDrop")))

  val obj2 = Dataset(name = "Test DS",
    version = 1,
    hdfsPath = "newPath",
    hdfsPublishPath = "newPublishPath",
    schemaName = "newSchema",
    schemaVersion = 1,
    conformance = List(LiteralConformanceRule(order = 0, controlCheckpoint = true, outputColumn = "something", value = "1.01")))

  test("Testing getPrimitiveFieldsAudit empty") {
    val emptyRes = obj1.getPrimitiveFieldsAudit(obj2, Seq())
    assertResult(Seq())(emptyRes)
  }
  
  test("Testing getPrimitiveFieldsAudit non-empty") {
    val primitiveRes = obj1.getPrimitiveFieldsAudit(obj2, Seq(AuditFieldName("hdfsPath", "HDFS Path"),
      AuditFieldName("schemaName", "Schema Name"),
      AuditFieldName("schemaVersion", "Schema Version")))
      
    val primitiveExp = Seq(
      AuditTrailChange(field = "hdfsPath", oldValue = Some("oldPath"), newValue = Some("newPath"), message = "HDFS Path updated."),
      AuditTrailChange(field = "schemaName", oldValue = Some("someSchema"), newValue = Some("newSchema"), message = "Schema Name updated."),
      AuditTrailChange(field = "schemaVersion", oldValue = Some("0"), newValue = Some("1"), message = "Schema Version updated."))
      
      assertResult(primitiveExp)(primitiveRes)
  }
  
  test("Testing getSeqFieldsAudit empty") {
    val emptyRes = obj1.getSeqFieldsAudit(obj2, AuditFieldName("abcdef", "abcdef"))
    assertResult(Seq())(emptyRes)
  }
  
  test("Testing getSeqFieldsAudit non-empty") {
    val emptyRes = obj1.getSeqFieldsAudit(obj2, AuditFieldName("conformance", "Conformance rule"))
    assertResult(Seq(
          AuditTrailChange(field = "conformance", oldValue = Some("DropConformanceRule(0,true,toDrop)"), None, message = "Conformance rule removed."),
          AuditTrailChange(field = "conformance", oldValue = None, newValue = Some("LiteralConformanceRule(0,something,true,1.01)"), message = "Conformance rule added.")
    ))(emptyRes)
  }  
}
