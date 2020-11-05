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

class SchemaFieldTest extends FunSuite {
  private val schemaFieldChildSecondLevel = SchemaField(
    name = "String",
    `type` = "string",
    path = "",
    elementType = None,
    containsNull = None,
    nullable = false,
    metadata = Map.empty,
    children = Seq.empty
  )

  private val schemaFieldChildOne = SchemaField(
    name = "AnyStruct2",
    `type` = "struct",
    path = "",
    elementType = None,
    containsNull = None,
    nullable = true,
    metadata = Map.empty,
    children = Seq(schemaFieldChildSecondLevel)
  )

  private val schemaFieldChildTwo = SchemaField(
    name = "Number",
    `type` = "ling",
    path = "AnyStruct",
    elementType = None,
    containsNull = None,
    nullable = true,
    metadata = Map.empty,
    children = Seq.empty
  )

  private val schemaFieldRoot = SchemaField(
    name = "AnyStruct",
    `type` = "struct",
    path = "",
    elementType = None,
    containsNull = None,
    nullable = true,
    metadata = Map.empty,
    children = Seq(schemaFieldChildOne, schemaFieldChildTwo)
  )

  test("testGetAllChildren") {
    val allChildren = List("String", "AnyStruct2", "AnyStruct.Number")
    assert(allChildren == schemaFieldRoot.getAllChildren)
  }

}
