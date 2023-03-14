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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.node.ArrayNode
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema => AosSchema}
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.backend.audit._
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter
import za.co.absa.enceladus.model.versionedModel.VersionedModel

import java.time.ZonedDateTime
import scala.annotation.meta.field
import scala.beans.BeanProperty

case class MappingTable(
  @(AosSchema@field)(example = "mappingTableA")
  @BeanProperty name: String,

  @(AosSchema@field)(example = "1")
  @BeanProperty version: Int = 1,

  @(AosSchema@field)(implementation = classOf[String], example = "MT description")
  @BeanProperty description: Option[String] = None,

  @(AosSchema@field)(example = "/input/path/for/mappingtable")
  @BeanProperty hdfsPath: String,

  @(AosSchema@field)(example = "schemaA")
  @BeanProperty schemaName: String,

  @(AosSchema@field)(example = "1")
  @BeanProperty schemaVersion: Int,

  @(ArraySchema@field)(schema = new AosSchema(implementation = classOf[DefaultValue]))
  @BeanProperty defaultMappingValue: List[DefaultValue] = List(),

  @BeanProperty dateCreated: ZonedDateTime = ZonedDateTime.now(),
  @(AosSchema@field)(example = "user1")
  @BeanProperty userCreated: String = null, //scalastyle:ignore null

  @BeanProperty lastUpdated: ZonedDateTime = ZonedDateTime.now(),
  @(AosSchema@field)(example = "user2")
  @BeanProperty userUpdated: String = null, //scalastyle:ignore null

  @(AosSchema@field)(example = "false")
  @BeanProperty disabled: Boolean = false,

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateDisabled: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String], example = "user3")
  @BeanProperty userDisabled: Option[String] = None,

  @(AosSchema@field)(implementation = classOf[Boolean], example = "true")
  @BeanProperty locked: Option[Boolean] = None,

  @(AosSchema@field)(implementation = classOf[ZonedDateTime])
  @BeanProperty dateLocked: Option[ZonedDateTime] = None,

  @(AosSchema@field)(implementation = classOf[String], example = "user4")
  @BeanProperty userLocked: Option[String] = None,

  @(AosSchema@field)(implementation = classOf[Reference])
  @BeanProperty parent: Option[Reference] = None,

  @(AosSchema@field)(implementation = classOf[DataFrameFilter], example = "{" +
    "\"_t\": \"EqualsFilter\"," +
    "\"columnName\": \"exampleColumn1\"," +
    "\"value\":\"wantedValue1\"," +
    "\"valueType\": \"string\"" +
    "}"
  )
  @BeanProperty filter: Option[DataFrameFilter] = None
) extends VersionedModel with Auditable[MappingTable] {

  override def setVersion(value: Int): MappingTable = this.copy(version = value)
  override def setDisabled(disabled: Boolean): VersionedModel = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): VersionedModel = this.copy(lastUpdated = time)
  override def setUpdatedUser(user: String): VersionedModel = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): VersionedModel = this.copy(description = desc)
  override def setDateCreated(time: ZonedDateTime): VersionedModel = this.copy(dateCreated = time)
  override def setUserCreated(user: String): VersionedModel = this.copy(userCreated = user)
  override def setDateDisabled(time: Option[ZonedDateTime]): VersionedModel = this.copy(dateDisabled = time)
  override def setUserDisabled(user: Option[String]): VersionedModel = this.copy(userDisabled = user)
  override def setLocked(locked: Option[Boolean]): VersionedModel = this.copy(locked = locked)
  override def setDateLocked(dateLocked: Option[ZonedDateTime]): VersionedModel = this.copy(dateLocked = dateLocked)
  override def setUserLocked(userLocked: Option[String]): VersionedModel = this.copy(userLocked = userLocked)
  def setSchemaName(newName: String): MappingTable = this.copy(schemaName = newName)
  def setSchemaVersion(newVersion: Int): MappingTable = this.copy(schemaVersion = newVersion)
  def setHdfsPath(newPath: String): MappingTable = this.copy(hdfsPath = newPath)
  def setDefaultMappingValue(newDefaults: List[DefaultValue]): MappingTable = this.copy(defaultMappingValue = newDefaults)
  override def setParent(newParent: Option[Reference]): MappingTable = this.copy(parent = newParent)
  def setFilter(newFilter: Option[DataFrameFilter]): MappingTable = copy(filter = newFilter)

  @JsonIgnore
  def getDefaultMappingValueAsMap: Map[String, String] = {
    defaultMappingValue.map(_.toTuple).toMap
  }

  override val createdMessage = AuditTrailEntry(ref = Reference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
    AuditTrailChange(field = "", oldValue = None, newValue = None, s"Mapping Table ${name} created.")))

  override def getAuditMessages(newRecord: MappingTable): AuditTrailEntry = {
    AuditTrailEntry(ref = Reference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("description", "Description"),
          AuditFieldName("hdfsPath", "HDFS Path"),
          AuditFieldName("schemaName", "Schema Name"),
          AuditFieldName("schemaVersion", "Schema Version"))) ++
        super.getSeqFieldsAudit(newRecord, AuditFieldName("defaultMappingValue", "Default Mapping Value")))
  }

  override def exportItem(): String = {
    val defaultMappingValueJsonList: ArrayNode = objectMapperBase.valueToTree(defaultMappingValue.toArray)

    val objectItemMapper = objectMapperRoot.withObject("/item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.put("hdfsPath", hdfsPath)
    objectItemMapper.put("schemaName", schemaName)
    objectItemMapper.put("schemaVersion", schemaVersion)
    objectItemMapper.putArray("defaultMappingValue").addAll(defaultMappingValueJsonList)

    objectMapperRoot.toString
  }
}
