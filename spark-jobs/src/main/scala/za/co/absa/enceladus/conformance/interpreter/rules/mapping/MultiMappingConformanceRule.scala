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

package za.co.absa.enceladus.conformance.interpreter.rules.mapping

import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter

case class MultiMappingConformanceRule(attributeMappings: Map[String, String], outputColumns: Map[String, String] = Map(),
                                       mappingTableFilter: Option[DataFrameFilter], overrideMappingTableOwnFilter: Boolean)
object MultiMappingConformanceRule {
  // output columns is expressed as outputColumn -> targetColumn,
  // since different target columns cannot point to the same output column
  def apply(rule: MappingConformanceRule): MultiMappingConformanceRule = {
    val mergedColumns = rule.getAllOutputColumns()
    MultiMappingConformanceRule(rule.attributeMappings, mergedColumns, rule.mappingTableFilter, rule.getOverrideMappingTableOwnFilter)
  }
}
