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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.node.ObjectNode
import io.swagger.v3.oas.annotations.media.{Schema => AosSchema}
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.backend.audit.{AuditFieldName, AuditTrailChange, AuditTrailEntry, Auditable}
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.propertyType.PropertyType
import za.co.absa.enceladus.model.versionedModel.VersionedModel

import java.time.ZonedDateTime
import scala.annotation.meta.field
import scala.beans.BeanProperty

case class PropertyDefinition(
  @(AosSchema@field)(example = "propertyDefinitionA")
  @BeanProperty name: String,

  @(AosSchema@field)(example = "1")
  @BeanProperty version: Int = 1,

  @(AosSchema@field)(implementation = classOf[String], example = "property definition description")
  @BeanProperty description: Option[String] = None,

  @(AosSchema@field)(implementation = classOf[PropertyType], example =
    "{\"_t\": \"StringPropertyType\",\"suggestedValue\": \"valueX\"}")
  @BeanProperty propertyType: PropertyType,

  @BeanProperty putIntoInfoFile: Boolean = false,

  @(AosSchema@field)(implementation = classOf[Essentiality], example = "{\"_t\": \"Optional\"}")
  @BeanProperty essentiality: Essentiality = Essentiality.Optional,

  @(AosSchema@field)(example = "false")
  @BeanProperty disabled: Boolean = false,

  @(AosSchema@field)(implementation = classOf[Boolean], example = "true")
  @BeanProperty locked: Option[Boolean] = None,

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateLocked: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String], example = "user1")
  @BeanProperty userLocked: Option[String] = None,

  @BeanProperty dateCreated: ZonedDateTime = ZonedDateTime.now(),
  @(AosSchema@field)(example = "user2")
  @BeanProperty userCreated: String = null, //scalastyle:ignore null

  @BeanProperty lastUpdated: ZonedDateTime = ZonedDateTime.now(),
  @(AosSchema@field)(example = "user3")
  @BeanProperty userUpdated: String = null, //scalastyle:ignore null

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateDisabled: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String], example = "user4")
  @BeanProperty userDisabled: Option[String] = None,

  @(AosSchema@field)(implementation = classOf[Reference])
  @BeanProperty parent: Option[Reference] = None
) extends VersionedModel with Auditable[PropertyDefinition] {

  @JsonIgnore
  def isRequired(allowRun: Boolean): Boolean = essentiality == Essentiality.Mandatory(allowRun)
  @JsonIgnore
  def isRecommended: Boolean = essentiality == Essentiality.Recommended
  @JsonIgnore
  def isOptional: Boolean = essentiality == Essentiality.Optional

  // VersionModel induced methods:
  override def setVersion(value: Int): PropertyDefinition = this.copy(version = value)
  override def setDisabled(disabled: Boolean): PropertyDefinition = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): PropertyDefinition = this.copy(lastUpdated = time)
  override def setUpdatedUser(user: String): PropertyDefinition = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): PropertyDefinition = this.copy(description = desc)
  override def setDateCreated(time: ZonedDateTime): PropertyDefinition = this.copy(dateCreated = time)
  override def setUserCreated(user: String): PropertyDefinition = this.copy(userCreated = user)
  override def setDateDisabled(time: Option[ZonedDateTime]): PropertyDefinition = this.copy(dateDisabled = time)
  override def setLocked(locked: Option[Boolean]): VersionedModel = this.copy(locked = locked)
  override def setDateLocked(dateLocked: Option[ZonedDateTime]): VersionedModel = this.copy(dateLocked = dateLocked)
  override def setUserLocked(userLocked: Option[String]): VersionedModel = this.copy(userLocked = userLocked)
  override def setUserDisabled(user: Option[String]): PropertyDefinition = this.copy(userDisabled = user)
  override def setParent(newParent: Option[Reference]): PropertyDefinition = this.copy(parent = newParent)

  def setEssentiality(newEssentiality: Essentiality): PropertyDefinition = this.copy(essentiality = newEssentiality)
  def setPropertyType(newPropertyType: PropertyType): PropertyDefinition = this.copy(propertyType = newPropertyType)
  def setPutIntoInfoFile(newPutIntoInfoFile: Boolean): PropertyDefinition = this.copy(putIntoInfoFile = newPutIntoInfoFile)

  // Auditable induced methods:
  override val createdMessage: AuditTrailEntry = AuditTrailEntry(ref = Reference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
      AuditTrailChange(field = "", oldValue = None, newValue = None, s"PropertyDefinition $name created.")))

  override def getAuditMessages(newRecord: PropertyDefinition): AuditTrailEntry = {
    AuditTrailEntry(ref = Reference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(
          AuditFieldName("description", "Description"),
          AuditFieldName("propertyType", "Property type"),
          AuditFieldName("putIntoInfoFile", "Put into _INFO file"),
          AuditFieldName("essentiality", "Essentiality")
        )
      )
    )
  }

  override def exportItem(): String = {
    // using objectMapperBase.writeValueAsString would work too, but the object would get "-escaped
    val propertyTypeJson: ObjectNode = objectMapperBase.valueToTree(propertyType)
    val essentialityJson: ObjectNode = objectMapperBase.valueToTree(essentiality)

    val objectItemMapper = objectMapperRoot.withObject("/item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.set("propertyType", propertyTypeJson)
    objectItemMapper.put("putIntoInfoFile", putIntoInfoFile)
    objectItemMapper.set("essentiality", essentialityJson)

    objectMapperRoot.toString
  }

}

