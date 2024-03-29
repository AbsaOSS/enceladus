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
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema => AosSchema}
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.versionedModel.VersionedModel
import za.co.absa.enceladus.model.backend.audit._
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.backend.scheduler.oozie.OozieSchedule

import scala.annotation.meta.field
import scala.beans.BeanProperty

case class Dataset(
  @(AosSchema@field)(example = "datasetA")
  @BeanProperty name: String,

  @(AosSchema@field)(example = "1")
  @BeanProperty version: Int = 1, // @BP generates getter (and setter) -> swagger is able to correctly report this field

  @(AosSchema@field)(implementation = classOf[String], example = "dataset description")
  @BeanProperty description: Option[String] = None,

  @(AosSchema@field)(example = "/input/path/for/dataset")
  @BeanProperty hdfsPath: String,

  @(AosSchema@field)(example = "/output/path/for/dataset")
  @BeanProperty hdfsPublishPath: String,

  @(AosSchema@field)(example = "schemaA")
  @BeanProperty schemaName: String,

  @(AosSchema@field)(example = "1")
  @BeanProperty schemaVersion: Int,

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

  @(ArraySchema@field)(schema = new AosSchema(implementation = classOf[ConformanceRule]))
  @BeanProperty conformance: List[ConformanceRule] = List.empty,

  @(AosSchema@field)(implementation = classOf[Reference])
  @BeanProperty parent: Option[Reference] = None,

  @(AosSchema@field)(implementation = classOf[OozieSchedule])
  @BeanProperty schedule: Option[OozieSchedule] = None, //To be used for backward versioning compatibility

  @(AosSchema@field)(implementation = classOf[java.util.Map[String, String]],
    example = "{\"field1\": \"value1\", \"field2\": \"value2\"}")
  @BeanProperty properties: Option[Map[String, String]] = Some(Map.empty),

  @(AosSchema@field)(implementation = classOf[Validation])
  @BeanProperty propertiesValidation: Option[Validation] = None
) extends VersionedModel with Auditable[Dataset] {

  override def setVersion(value: Int): Dataset = this.copy(version = value)
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

  def setSchemaName(newName: String): Dataset = this.copy(schemaName = newName)
  def setSchemaVersion(newVersion: Int): Dataset = this.copy(schemaVersion = newVersion)
  def setHdfsPath(newPath: String): Dataset = this.copy(hdfsPath = newPath)
  def setHdfsPublishPath(newPublishPath: String): Dataset = this.copy(hdfsPublishPath = newPublishPath)
  def setConformance(newConformance: List[ConformanceRule]): Dataset = this.copy(conformance = newConformance)
  def setProperties(newProperties: Option[Map[String, String]]): Dataset = this.copy(properties = newProperties)
  override def setParent(newParent: Option[Reference]): Dataset = this.copy(parent = newParent)

  def propertiesAsMap: Map[String, String] = properties.getOrElse(Map.empty)

  /**
   * @return a dataset with it's mapping conformance rule attributeMappings where the dots are
   *         <MappingConformanceRule.DOT_REPLACEMENT_SYMBOL>
   */
  def encode: Dataset = substituteMappingConformanceRuleCharacter(this, replaceInMapKeys('.', MappingConformanceRule.DotReplacementSymbol))

  /**
   * @return a dataset with it's mapping conformance rule attributeMappings where the
   *         <MappingConformanceRule.DOT_REPLACEMENT_SYMBOL> are dots
   */
  def decode: Dataset = substituteMappingConformanceRuleCharacter(this, replaceInMapKeys(MappingConformanceRule.DotReplacementSymbol, '.'))

  override val createdMessage: AuditTrailEntry = AuditTrailEntry(
    ref = Reference(collection = None, name = name, version = version),
    updatedBy = userUpdated, updated = lastUpdated, changes = Seq(
      AuditTrailChange(field = "", oldValue = None, newValue = None, s"Dataset $name created."))
  )

  def replaceInMapKeys(from: Char, to: Char)(map: Map[String, String]): Map[String, String] = map.map(key => {
    (key._1.replace(from, to), key._2)
  })

  private def substituteMappingConformanceRuleCharacter(dataset: Dataset,
                                                        replaceInMapKeys: Map[String, String] => Map[String, String]): Dataset = {
    val conformanceRules = dataset.conformance.map {
      case m: MappingConformanceRule =>
        m.copy(attributeMappings = replaceInMapKeys(m.attributeMappings),
          additionalColumns = m.additionalColumns.map(replaceInMapKeys))
      case c: ConformanceRule => c
    }

    dataset.copy(conformance = conformanceRules)
  }

  override def getAuditMessages(newRecord: Dataset): AuditTrailEntry = {
    AuditTrailEntry(ref = Reference(collection = None, name = newRecord.name, version = newRecord.version),
      updated = newRecord.lastUpdated,
      updatedBy = newRecord.userUpdated,
      changes = super.getPrimitiveFieldsAudit(newRecord,
        Seq(AuditFieldName("description", "Description"),
          AuditFieldName("hdfsPath", "HDFS Path"),
          AuditFieldName("hdfsPublishPath", "HDFS Publish Path"),
          AuditFieldName("schemaName", "Schema Name"),
          AuditFieldName("schemaVersion", "Schema Version"),
          AuditFieldName("schedule", "Schedule"))) ++
        super.getSeqFieldsAudit(newRecord, AuditFieldName("conformance", "Conformance rule")) ++
        super.getOptionalMapFieldsAudit(newRecord, AuditFieldName("properties", "Property")))
  }

  override def exportItem(): String = {
    val conformanceJsonList: ArrayNode = objectMapperBase.valueToTree(conformance.toArray)
    val propertiesJsonList: ObjectNode = objectMapperBase.valueToTree(propertiesAsMap)

    val objectItemMapper = objectMapperRoot.withObject("/item")

    objectItemMapper.put("name", name)
    description.map(d => objectItemMapper.put("description", d))
    objectItemMapper.put("hdfsPath", hdfsPath)
    objectItemMapper.put("hdfsPublishPath", hdfsPublishPath)
    objectItemMapper.put("schemaName", schemaName)
    objectItemMapper.put("schemaVersion", schemaVersion)
    objectItemMapper.putArray("conformance").addAll(conformanceJsonList)
    objectItemMapper.putObject("properties").setAll(propertiesJsonList)

    objectMapperRoot.toString
  }
}
