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

import java.time.ZonedDateTime

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.menas.audit.{AuditFieldName, AuditTrailChange, AuditTrailEntry, Auditable}
import za.co.absa.enceladus.model.properties.essentiality.{Essentiality, Mandatory, Optional}
import za.co.absa.enceladus.model.properties.propertyType.PropertyType
import za.co.absa.enceladus.model.versionedModel.VersionedModel

case class PropertyDefinition(name: String,
                              version: Int = 1,
                              description: Option[String] = None,

                              propertyType: PropertyType,
                              suggestedValue: String = "",
                              putIntoInfoFile: Boolean = false,
                              essentiality: Essentiality = Optional(), // consider renaming or simplifying into 2 boolean fields?
                              disabled: Boolean = false,

                              // VersionModel induced fields:
                              dateCreated: ZonedDateTime = ZonedDateTime.now(),
                              userCreated: String = null,

                              lastUpdated: ZonedDateTime = ZonedDateTime.now(),
                              userUpdated: String = null,

                              dateDisabled: Option[ZonedDateTime] = None,
                              userDisabled: Option[String] = None,
                              parent: Option[MenasReference] = None
                             ) extends VersionedModel with Auditable[PropertyDefinition] {

  require(propertyType.isValueConforming(suggestedValue),
    s"The suggested value '$suggestedValue' does not conform to the propertyType $propertyType!")

  def typeSpecificSettings: Map[String, Any] = propertyType.typeSpecificSettings

  val required: Boolean = essentiality == Mandatory()
  val optional: Boolean = essentiality == Optional()

  // VersionModel induced methods:
  override def setVersion(value: Int): PropertyDefinition = this.copy(version = value)
  override def setDisabled(disabled: Boolean): PropertyDefinition = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): PropertyDefinition = this.copy(lastUpdated = time)
  override def setUpdatedUser(user: String): PropertyDefinition = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): PropertyDefinition = this.copy(description = desc)
  override def setDateCreated(time: ZonedDateTime): PropertyDefinition = this.copy(dateCreated = time)
  override def setUserCreated(user: String): PropertyDefinition = this.copy(userCreated = user)
  override def setDateDisabled(time: Option[ZonedDateTime]): PropertyDefinition = this.copy(dateDisabled = time)
  override def setUserDisabled(user: Option[String]): PropertyDefinition = this.copy(userDisabled = user)
  override def setParent(newParent: Option[MenasReference]): PropertyDefinition = this.copy(parent = newParent)

  // Auditable induced methods:
  override val createdMessage = AuditTrailEntry(menasRef = MenasReference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
      AuditTrailChange(field = "", oldValue = None, newValue = None, s"PropertyDefinition $name created.")))

  override def getAuditMessages(newRecord: PropertyDefinition): AuditTrailEntry = {
    AuditTrailEntry(menasRef = MenasReference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("propertyType", "Property type"),
          AuditFieldName("suggestedValue", "Suggested value"),
          AuditFieldName("putIntoInfoFile", "Put into _INFO file"),
          AuditFieldName("essentiality", "Essentiality"),
          AuditFieldName("schemaVersion", "Schema Version"),
          AuditFieldName("schedule", "Schedule"))))
    // todo property type in depth audit?
  }

  override def exportItem(): String = {
    // using objectMapperBase.writeValueAsString would work too, but the object would get "-escaped
    val propertyTypeJson: ObjectNode = objectMapperBase.valueToTree(propertyType)
    val essentialityJson: ObjectNode = objectMapperBase.valueToTree(essentiality)
    val typeSpecificSettingsJson: ArrayNode = objectMapperBase.valueToTree(typeSpecificSettings.toArray)

    val objectItemMapper = objectMapperRoot.`with`("item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.set("propertyType", propertyTypeJson)
    objectItemMapper.put("suggestedValue", suggestedValue)
    objectItemMapper.put("putIntoInfoFile", putIntoInfoFile)
    objectItemMapper.set("essentiality", essentialityJson)
    objectItemMapper.putArray("typeSpecificSettings").addAll(typeSpecificSettingsJson)

    objectMapperRoot.toString
  }

}

