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

package za.co.absa.enceladus.model.properties

import za.co.absa.enceladus.model.properties.essentiality.Essentiality

case class PropertyDefinitionStats(name: String,
                                   version: Int = 1,
                                   essentiality: Essentiality = Essentiality.Optional,
                                   missingInDatasetsCount: Int = 0
) {
  def setName(value: String): PropertyDefinitionStats = this.copy(name = value)
  def setVersion(value: Int): PropertyDefinitionStats = this.copy(version = value)
  def setEssentiality(value: Essentiality): PropertyDefinitionStats = this.copy(essentiality = value)
  def setMissingInDatasetsCount(disabled: Int): PropertyDefinitionStats = this.copy(missingInDatasetsCount = disabled)
}

object PropertyDefinitionStats {
  def apply(propertyDefinition: PropertyDefinition, missingCounts: Int): PropertyDefinitionStats = {
    PropertyDefinitionStats(propertyDefinition.name, propertyDefinition.version,
      propertyDefinition.essentiality, missingCounts)
  }
}
