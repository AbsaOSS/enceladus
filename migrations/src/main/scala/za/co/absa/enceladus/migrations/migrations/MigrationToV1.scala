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

package za.co.absa.enceladus.migrations.migrations

import java.time.ZonedDateTime

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.migrations.framework.migration._
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.{DefaultValue, Serializer1}

import scala.util.control.NonFatal

/**
  * Migration from Menas 0.* to Menas 1.0 model
  */
object MigrationToV1 extends MigrationBase with CollectionMigration with JsonMigration with CommandMigration {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val migrationUserName = "migration"

  override val targetVersion: Int = 1

  createCollection("attachment")

  createIndex("dataset", Seq(IndexField("name", ASC), IndexField("version", ASC)), unique = true)
  createIndex("schema", Seq(IndexField("name", ASC), IndexField("version", ASC)), unique = true)
  createIndex("mapping_table", Seq(IndexField("name", ASC), IndexField("version", ASC)), unique = true)
  createIndex("run", Seq(IndexField("dataset", ASC), IndexField("datasetVersion", ASC), IndexField("runId", ASC)), unique = true)
  createIndex("attachment", Seq(IndexField("refName", ASC), IndexField("refVersion", ASC)))

  // rename and add dir sizes in additionalInfo
  runCommand("run") (versionedCollectionName => {
    s"""{
       |update: "$versionedCollectionName",
       |updates: [
       |  {
       |    q: {"controlMeasure.metadata.additionalInfo.raw_dir_size": {$$exists: true}},
       |    u:  { $$rename: {
       |      "controlMeasure.metadata.additionalInfo.raw_dir_size":
       |      "controlMeasure.metadata.additionalInfo.std_input_dir_size"
       |    } },
       |    upsert: false,
       |    multi: true
       |  },{
       |    q: {"controlMeasure.metadata.additionalInfo.std_dir_size": {$$exists: true}},
       |    u:  { $$addFields: [
       |        {"controlMeasure.metadata.additionalInfo.conform_input_dir_size":
       |        "$$controlMeasure.metadata.additionalInfo.std_dir_size"}
       |      ] },
       |    upsert: false,
       |    multi: true
       |  },{
       |    q: {"controlMeasure.metadata.additionalInfo.std_dir_size": {$$exists: true}},
       |    u:  { $$rename: {
       |      "controlMeasure.metadata.additionalInfo.std_dir_size":
       |      "controlMeasure.metadata.additionalInfo.std_output_dir_size"
       |    } },
       |    upsert: false,
       |    multi: true
       |  },{
       |    q: {"controlMeasure.metadata.additionalInfo.publish_dir_size": {$$exists: true}},
       |    u:  { $$rename: {
       |      "controlMeasure.metadata.additionalInfo.publish_dir_size":
       |      "controlMeasure.metadata.additionalInfo.conform_output_dir_size"
       |    } },
       |    upsert: false,
       |    multi: true
       |  }
       |],
       |ordered: false,
       |}""".stripMargin
  })

  transformJSON("schema")(model0Json => {
    try {
      val schema0 = Serializer0.deserializeSchema(model0Json)

      val schema1 = model1.Schema(
        schema0.name,
        schema0.version,
        None,
        userCreated = migrationUserName,
        userUpdated = migrationUserName,
        fields = schema0.fields.map(convertSchemaField(_, Nil))
      )

      Serializer1.serializeSchema(schema1)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for 'schema': ${e.getMessage}")
        InvalidDocument
    }
  })

  transformJSON("mapping_table")(model0Json => {
    try {
      val mappingTable0 = Serializer0.deserializeMappingTable(model0Json)

      val defaultValueList = mappingTable0.defaultMappingValue match {
        case Some(value) => List(DefaultValue("*", value))
        case None => Nil
      }

      val mappingTable1 = model1.MappingTable(
        mappingTable0.name,
        mappingTable0.version,
        None,
        mappingTable0.hdfsPath,
        mappingTable0.schemaName,
        mappingTable0.schemaVersion,
        defaultValueList,
        ZonedDateTime.now(),
        migrationUserName,
        ZonedDateTime.now(),
        migrationUserName
      )

      Serializer1.serializeMappingTable(mappingTable1)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for 'mapping_table': ${e.getMessage}")
        InvalidDocument
    }
  })

  transformJSON("dataset")(model0Json => {
    val fixJson = model0Json.replaceAll("\"jsonClass\"\\w*:", "\"_t\" :")

    try {
      val dataset0 = Serializer0.deserializeDataset(fixJson)

      val dataset1 = model1.Dataset(
        dataset0.name,
        dataset0.version,
        None,
        dataset0.hdfsPath,
        dataset0.hdfsPublishPath,
        dataset0.schemaName,
        dataset0.schemaVersion,
        ZonedDateTime.now(),
        migrationUserName,
        ZonedDateTime.now(),
        migrationUserName,
        conformance = dataset0.conformance.map(convertConformanceRule)
      )

      Serializer1.serializeDataset(dataset1)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error for 'dataset': ${e.getMessage}")
        InvalidDocument
    }
  })

  /**
    * Converts a Model 0 conformance rule into Model 1 conformance rule
    */
  def convertConformanceRule(rule: model0.conformanceRule.ConformanceRule): model1.conformanceRule.ConformanceRule = {
    rule match {
      case model0.conformanceRule.CastingConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, outputDataType) =>
        model1.conformanceRule.CastingConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, outputDataType, "CastingConformanceRule")

      case model0.conformanceRule.ConcatenationConformanceRule(order, outputColumn, controlCheckpoint, inputColumns) =>
        model1.conformanceRule.ConcatenationConformanceRule(order, outputColumn, controlCheckpoint, inputColumns, "ConcatenationConformanceRule")

      case model0.conformanceRule.DropConformanceRule(order, controlCheckpoint, outputColumn) =>
        model1.conformanceRule.DropConformanceRule(order, controlCheckpoint, outputColumn, "DropConformanceRule")

      case model0.conformanceRule.LiteralConformanceRule(order, outputColumn, controlCheckpoint, value) =>
        model1.conformanceRule.LiteralConformanceRule(order, outputColumn, controlCheckpoint, value, "LiteralConformanceRule")

      case model0.conformanceRule.MappingConformanceRule(order, controlCheckpoint, mappingTable, mappingTableVersion, attributeMappings, targetAttribute, outputColumn, isNullSafe) =>
        model1.conformanceRule.MappingConformanceRule(order, controlCheckpoint, mappingTable, mappingTableVersion, attributeMappings, targetAttribute, outputColumn, isNullSafe, "MappingConformanceRule")

      case model0.conformanceRule.NegationConformanceRule(order, outputColumn, controlCheckpoint, inputColumn) =>
        model1.conformanceRule.NegationConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, "NegationConformanceRule")

      case model0.conformanceRule.SingleColumnConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, inputColumnAlias) =>
        model1.conformanceRule.SingleColumnConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, inputColumnAlias, "SingleColumnConformanceRule")

      case model0.conformanceRule.SparkSessionConfConformanceRule(order, outputColumn, controlCheckpoint, sparkConfKey) =>
        model1.conformanceRule.SparkSessionConfConformanceRule(order, outputColumn, controlCheckpoint, sparkConfKey, "SparkSessionConfConformanceRule")

      case model0.conformanceRule.UppercaseConformanceRule(order, outputColumn, controlCheckpoint, inputColumn) =>
        model1.conformanceRule.UppercaseConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, "UppercaseConformanceRule")
      case _ =>
        sys.error(s"Unknown conformance rule encountered $rule.")
    }
  }

  /**
    * Converts a Model 0 schema field into Model 1 schema field
    */
  def convertSchemaField(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    field0.`type` match {
      case "array" => convertSchemaFieldArray(field0, path :+ field0.name)
      case "struct" => convertSchemaFieldStruct(field0, path :+ field0.name)
      case _ => convertSchemaFieldPrimitive(field0, path :+ field0.name)
    }
  }

  /**
    * Schema field conversion helper for primitive fields
    */
  private def convertSchemaFieldPrimitive(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
      field0.nullable, field0.metadata, Nil)
  }

  /**
    * Schema field conversion helper for struct fields
    */
  private def convertSchemaFieldStruct(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
      field0.nullable, field0.metadata, field0.children.map(convertSchemaField(_, path)))
  }

  /**
    * Schema field conversion helper for array fields
    */
  private def convertSchemaFieldArray(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    field0.elementType.get match {
      case "array" => convertSchemaFieldArray(field0.children.head, path)
      case "struct" => convertSchemaFieldStruct(field0, path)
      case _ =>
        model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
          field0.nullable, field0.metadata, Nil)
    }
  }

}
