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

package za.co.absa.enceladus.model.conformanceRule

case class MappingConformanceRule(
    order: Int,
    controlCheckpoint: Boolean,
    mappingTable: String,
    mappingTableVersion: Int,
    attributeMappings: Map[String, String], // key = mapping table column, value = input df column
    targetAttribute: String,
    outputColumn: String,
    isNullSafe: Boolean = false) extends ConformanceRule 

object MappingConformanceRule {
  // attributeMappings property has key's with dot's that mongo doesn't accept; this symbol is used to replace the dots
  val DOT_REPLACEMENT_SYMBOL: Char = '^'
}

