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

import java.time.ZonedDateTime

import com.fasterxml.jackson.databind.node.ArrayNode
import za.co.absa.enceladus.model.versionedModel.VersionedModel
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.menas.MenasReference

case class Schema(name: String,
    version: Int = 1,
    description: Option[String],

    dateCreated: ZonedDateTime = ZonedDateTime.now(),
    userCreated: String = null,

    lastUpdated: ZonedDateTime = ZonedDateTime.now(),
    userUpdated: String = null,

    disabled: Boolean = false,
    dateDisabled: Option[ZonedDateTime] = None,
    userDisabled: Option[String] = None,

    locked: Option[Boolean] = None,

    fields: List[SchemaField] = List(),
    parent: Option[MenasReference] = None) extends VersionedModel with Auditable[Schema] {

  override def setVersion(value: Int): Schema = this.copy(version = value)
  override def setDisabled(disabled: Boolean): VersionedModel = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): VersionedModel = this.copy(lastUpdated = time)
  override def setDateCreated(time: ZonedDateTime): VersionedModel = this.copy(dateCreated = time)
  override def setUserCreated(user: String): VersionedModel = this.copy(userCreated = user)
  override def setUpdatedUser(user: String): VersionedModel = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): VersionedModel = this.copy(description = desc)
  override def setDateDisabled(time: Option[ZonedDateTime]): VersionedModel = this.copy(dateDisabled = time)
  override def setLocked(locked: Option[Boolean]): VersionedModel = this.copy(locked = locked)
  override def setUserDisabled(user: Option[String]): VersionedModel = this.copy(userDisabled = user)
  override def setParent(newParent: Option[MenasReference]): Schema = this.copy(parent = newParent)

  override val createdMessage = AuditTrailEntry(menasRef = MenasReference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
    AuditTrailChange(field = "", oldValue = None, newValue = None, s"Schema $name created.")))

  override def getAuditMessages(newRecord: Schema): AuditTrailEntry = {
    AuditTrailEntry(menasRef = MenasReference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("description", "Description"))) ++
        super.getSeqFieldsAudit(newRecord, AuditFieldName("fields", "Schema field")))
  }

  override def exportItem(): String = {
    val fieldsJsonList: ArrayNode = objectMapperBase.valueToTree(fields.toArray)

    val objectItemMapper = objectMapperRoot.`with`("item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.putArray("fields").addAll(fieldsJsonList)

    objectMapperRoot.toString
  }
}
