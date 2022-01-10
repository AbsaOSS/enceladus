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

import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.model.conformanceRule.{DropConformanceRule, LiteralConformanceRule, MappingConformanceRule, SingleColumnConformanceRule}

class ConformedSchemaTest extends AnyFunSuite{
  private val conformanceRule1 = LiteralConformanceRule(
    order = 0,
    controlCheckpoint = true,
    outputColumn = "something",
    value = "1.01"
  )

  private val conformanceRule1d = LiteralConformanceRule(
    order = 0,
    controlCheckpoint = true,
    outputColumn = "fieldToDelete",
    value = "1.01"
  )

  private val conformanceRule2 = DropConformanceRule(order = 0,
    controlCheckpoint = true,
    outputColumn = "fieldToDelete")

  private val conformanceRule3 = MappingConformanceRule(order = 0,
    controlCheckpoint = true,
    outputColumn = "something3",additionalColumns = Some(Map("newCol" -> "mappedCol")),
    mappingTable = "",mappingTableVersion = 1,
    attributeMappings = Map(),targetAttribute = "col")

  private val conformanceRule4 = SingleColumnConformanceRule(
    order = 0,
    outputColumn = "singleCol",inputColumn = "as",
    inputColumnAlias = "subCol", controlCheckpoint = false)

  private val dataset = Dataset(name = "Test DS",
    version = 1,
    hdfsPath = "newPath",
    hdfsPublishPath = "newPublishPath",
    schemaName = "newSchema",
    schemaVersion = 1,
    conformance = List(conformanceRule1, conformanceRule1d, conformanceRule2, conformanceRule3, conformanceRule4),
    properties = Some(Map(
      "property1" -> "value1",
      "property2.sub" -> "value2"
    )
    ))

  val schemaFields = List(StructField("stdField",StringType))

  test("conformed schema") {
    val conformedSchema = ConformedSchema(schemaFields, dataset)
    assertResult(conformedSchema.hasField("stdField"))(true)
    assertResult(conformedSchema.hasField("fieldToDelete"))(false)
    assertResult(conformedSchema.hasField("something"))(true)
    assertResult(conformedSchema.hasField("newCol"))(true)
    assertResult(conformedSchema.hasField("newCol1"))(false)
    assertResult(conformedSchema.hasField("mappedColCol1"))(false)
    assertResult(conformedSchema.hasField("something3"))(true)
    assertResult(conformedSchema.hasField("col"))(false)
    assertResult(conformedSchema.hasField("singleCol"))(true)
    assertResult(conformedSchema.hasField("singleCol.subCol"))(true)
  }
}
