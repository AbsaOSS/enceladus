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

import org.scalatest.FunSuite

class SchemaTest extends FunSuite {
  private val schemaField = SchemaField(
    name = "someField",
    `type` = "StringType",
    path = "",
    elementType = None,
    containsNull = None,
    nullable = true,
    metadata = Map.empty,
    children = Seq.empty
  )

  private val schema = Schema(
    name = "SomeSchema",
    version = 2,
    description = Some("Some Desc"),
    fields = List(schemaField)
  )

  private val modelVersion = ModelVersion

  private val expectedSchema =
    s"""{"metadata":{"exportVersion":$modelVersion},"item":{"name":"SomeSchema","description":"Some Desc","fields":
      |[{"name":"someField","type":"StringType","path":"","elementType":null,"containsNull":null,
      |"nullable":true,"metadata":{},"children":[],"absolutePath":"someField"}]}}""".stripMargin.replaceAll("[\\r\\n]", "")

  test("export Schema") {
    assert(expectedSchema == schema.exportItem())
  }
}
