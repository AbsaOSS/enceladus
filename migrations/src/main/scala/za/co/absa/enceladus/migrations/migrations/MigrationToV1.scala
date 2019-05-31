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

import org.apache.log4j.{LogManager, Logger}
import za.co.absa.enceladus.migrations.framework.migration.{JsonMigration, MigrationBase}
import za.co.absa.enceladus.migrations.migrations.model0.Serializer0
import za.co.absa.enceladus.migrations.migrations.model1.Serializer1

import scala.util.control.NonFatal

/**
  * Migration from Menas 0.* to Menas 1.0 model
  */
object MigrationToV1 extends MigrationBase with JsonMigration {

  private val log: Logger = LogManager.getLogger(this.getClass)

  override val targetVersion: Int = 1

  transformJSON("schema")(model0Json => {
    try {
      val schema0 = Serializer0.deserialize(model0Json)

      val schema1 = model1.Schema(
        schema0.name,
        schema0.version,
        None,
        userCreated = "migration",
        userUpdated = "migration",
        disabled = false,
        fields = schema0.fields.map(convertSchemaField(_, Nil))
      )

      Serializer1.serialize(schema1)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Encountered a serialization error: ${e.getMessage}")
        InvalidDocument
    }
  })


  /**
    * Converts a Model 0 schema field into Model 1 schema field
    */
  private def convertSchemaField(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    field0.`type` match {
      case "array"  => convertSchemaFieldArray(field0, path :+ field0.name)
      case "struct" => convertSchemaFieldStruct(field0, path :+ field0.name)
      case _        => convertSchemaFieldPrimitive(field0, path :+ field0.name)
    }
  }

  private def convertSchemaFieldPrimitive(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
      field0.nullable, field0.metadata, Nil)
  }

  private def convertSchemaFieldStruct(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
      field0.nullable, field0.metadata, field0.children.map(convertSchemaField(_, path)))
  }

  private def convertSchemaFieldArray(field0: model0.SchemaField, path: List[String]): model1.SchemaField = {
    field0.elementType.get match {
      case "array"  => convertSchemaFieldArray(field0, path)
      case "struct" =>
        convertSchemaFieldStruct(field0, path)
      case _        =>
        model1.SchemaField(field0.name, field0.`type`, path.mkString("."), field0.elementType, field0.containsNull,
          field0.nullable, field0.metadata, Nil)
    }
  }

}
