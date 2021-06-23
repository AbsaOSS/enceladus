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

package za.co.absa.enceladus.model.menas.audit

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.{DropConformanceRule, LiteralConformanceRule}
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule

class AuditableTest extends AnyFunSuite {
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

  val obj3 = Dataset(name = "Test DS",
    version = 3,
    hdfsPath = "newPath",
    hdfsPublishPath = "newPublishPath",
    schemaName = "newSchema",
    schemaVersion = 1,
    conformance = List(LiteralConformanceRule(order = 0, controlCheckpoint = true, outputColumn = "something", value = "1.01")),
    properties = Some(Map("key1" -> "value1")))

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

  test("Testing getSeqFieldsAudit removed rule") {
    val removed = obj2.copy(conformance = List[ConformanceRule]())
    val removedRes = obj2.getSeqFieldsAudit(removed, AuditFieldName("conformance", "Conformance rule"))
    assertResult(Seq(
          AuditTrailChange(field = "conformance", oldValue = Some("LiteralConformanceRule(0,something,true,1.01)"), None, message = "Conformance rule removed.")
    ))(removedRes)
  }

  test("Testing added getOptionalMapFieldsAudit") {
    val result = obj2.getOptionalMapFieldsAudit(obj3, AuditFieldName("properties", "Property"))
    assertResult(Seq(
      AuditTrailChange(field = "properties", newValue = Some("""key: "key1" value: "value1""""), oldValue = None, message = "Property added.")
    ))(result)
  }

  test("Testing changed getOptionalMapFieldsAudit") {
    val newObj2 = obj2.copy(properties = Some(Map("key1" -> "value previous")))
    val result = newObj2.getOptionalMapFieldsAudit(obj3, AuditFieldName("properties", "Property"))
    assertResult(Seq(
      AuditTrailChange(field = "properties", newValue = Some("""key: "key1" value: "value1""""), oldValue = Some("""key: "key1" value: "value previous""""), message = "Property changed.")
    ))(result)
  }

  test("Testing removed getOptionalMapFieldsAudit") {
    val result = obj3.getOptionalMapFieldsAudit(obj2, AuditFieldName("properties", "Property"))
    assertResult(Seq(
      AuditTrailChange(field = "properties", newValue = None, oldValue = Some("""key: "key1" value: "value1""""), message = "Property removed.")
    ))(result)
  }

  test("Testing added getOptionalMapFieldsAudit - explicitly None properties") {
    val result = obj2.copy(properties = None).getOptionalMapFieldsAudit(obj3, AuditFieldName("properties", "Property"))
    assertResult(Seq(
      AuditTrailChange(field = "properties", newValue = Some("""key: "key1" value: "value1""""), oldValue = None, message = "Property added.")
    ))(result)
  }

  test("Testing removed getOptionalMapFieldsAudit  - explicitly None properties") {
    val result = obj3.getOptionalMapFieldsAudit(obj2.copy(properties = None), AuditFieldName("properties", "Property"))
    assertResult(Seq(
      AuditTrailChange(field = "properties", newValue = None, oldValue = Some("""key: "key1" value: "value1""""), message = "Property removed.")
    ))(result)
  }
}
