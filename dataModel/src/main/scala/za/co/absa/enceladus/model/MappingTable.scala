/*
 * Copyright 2018-2019 ABSA Group Limited
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
import za.co.absa.enceladus.model.versionedModel.VersionedModel
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.menas.MenasReference

case class MappingTable(name: String,
    version: Int = 0,
    description: Option[String] = None,

    hdfsPath: String,

    schemaName: String,
    schemaVersion: Int,

    defaultMappingValue: List[DefaultValue] = List(),

    dateCreated: ZonedDateTime = ZonedDateTime.now(),
    userCreated: String = null,

    lastUpdated: ZonedDateTime = ZonedDateTime.now(),
    userUpdated: String = null,

    disabled: Boolean = false,
    dateDisabled: Option[ZonedDateTime] = None,
    userDisabled: Option[String] = None,
    parent: Option[MenasReference] = None) extends VersionedModel with Auditable[MappingTable] {

  override def setVersion(value: Int): MappingTable = this.copy(version = value)
  override def setDisabled(disabled: Boolean): VersionedModel = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): VersionedModel = this.copy(lastUpdated = time)
  override def setUpdatedUser(user: String): VersionedModel = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): VersionedModel = this.copy(description = desc)
  override def setDateCreated(time: ZonedDateTime): VersionedModel = this.copy(dateCreated = time)
  override def setUserCreated(user: String): VersionedModel = this.copy(userCreated = user)
  def setSchemaName(newName: String) = this.copy(schemaName = newName)
  def setSchemaVersion(newVersion: Int) = this.copy(schemaVersion = newVersion)
  def setHDFSPath(newPath: String) = this.copy(hdfsPath = newPath)
  def setDefaultMappingValue(newDefaults: List[DefaultValue]) = this.copy(defaultMappingValue = newDefaults)
  override def setParent(newParent: Option[MenasReference]) = this.copy(parent = newParent)

  def getDefaultMappingValues: Map[String, String] = {
    defaultMappingValue.map(_.toTouple()).toMap
  }

  override val createdMessage = AuditTrailEntry(menasRef = MenasReference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
    AuditTrailChange(field = "", oldValue = None, newValue = None, s"Mapping Table ${name} created.")))

  override def getAuditMessages(newRecord: MappingTable): AuditTrailEntry = {
    AuditTrailEntry(menasRef = MenasReference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("description", "Description"),
          AuditFieldName("hdfsPath", "HDFS Path"),
          AuditFieldName("schemaName", "Schema Name"),
          AuditFieldName("schemaVersion", "Schema Version"))) ++
        super.getSeqFieldsAudit(newRecord, AuditFieldName("defaultMappingValue", "Default Mapping Value")))
  }
}
