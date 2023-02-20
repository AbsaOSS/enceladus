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

import com.fasterxml.jackson.databind.node.ArrayNode
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema => AosSchema}
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.backend.audit._
import za.co.absa.enceladus.model.versionedModel.VersionedModel

import java.time.ZonedDateTime
import scala.annotation.meta.field
import scala.beans.BeanProperty

case class Schema(name: String,
  @BeanProperty version: Int = 1,
  @(AosSchema@field)(implementation = classOf[String])
  @BeanProperty description: Option[String],

  @BeanProperty dateCreated: ZonedDateTime = ZonedDateTime.now(),
  @BeanProperty userCreated: String = null,

  @BeanProperty lastUpdated: ZonedDateTime = ZonedDateTime.now(),
  @BeanProperty userUpdated: String = null,

  @BeanProperty disabled: Boolean = false,

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateDisabled: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String])
  @BeanProperty userDisabled: Option[String] = None,

  @(AosSchema@field)(implementation = classOf[Boolean])
  @BeanProperty locked: Option[Boolean] = None,

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateLocked: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String])
  @BeanProperty userLocked: Option[String] = None,

  @(ArraySchema@field)(schema = new AosSchema(implementation = classOf[SchemaField]))
  @BeanProperty fields: List[SchemaField] = List(),

  @(AosSchema@field)(implementation = classOf[Reference])
  @BeanProperty parent: Option[Reference] = None
) extends VersionedModel with Auditable[Schema] {

  override def setVersion(value: Int): Schema = this.copy(version = value)
  override def setDisabled(disabled: Boolean): VersionedModel = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): VersionedModel = this.copy(lastUpdated = time)
  override def setDateCreated(time: ZonedDateTime): VersionedModel = this.copy(dateCreated = time)
  override def setUserCreated(user: String): VersionedModel = this.copy(userCreated = user)
  override def setUpdatedUser(user: String): VersionedModel = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): VersionedModel = this.copy(description = desc)
  override def setDateDisabled(time: Option[ZonedDateTime]): VersionedModel = this.copy(dateDisabled = time)
  override def setLocked(locked: Option[Boolean]): VersionedModel = this.copy(locked = locked)
  override def setDateLocked(dateLocked: Option[ZonedDateTime]): VersionedModel = this.copy(dateLocked = dateLocked)
  override def setUserLocked(userLocked: Option[String]): VersionedModel = this.copy(userLocked = userLocked)
  override def setUserDisabled(user: Option[String]): VersionedModel = this.copy(userDisabled = user)
  override def setParent(newParent: Option[Reference]): Schema = this.copy(parent = newParent)

  override val createdMessage = AuditTrailEntry(ref = Reference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
    AuditTrailChange(field = "", oldValue = None, newValue = None, s"Schema $name created.")))

  override def getAuditMessages(newRecord: Schema): AuditTrailEntry = {
    AuditTrailEntry(ref = Reference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("description", "Description"))) ++
        super.getSeqFieldsAudit(newRecord, AuditFieldName("fields", "Schema field")))
  }

  override def exportItem(): String = {
    val fieldsJsonList: ArrayNode = objectMapperBase.valueToTree(fields.toArray)

    val objectItemMapper = objectMapperRoot.withObject("/item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.putArray("fields").addAll(fieldsJsonList)

    objectMapperRoot.toString
  }
}
