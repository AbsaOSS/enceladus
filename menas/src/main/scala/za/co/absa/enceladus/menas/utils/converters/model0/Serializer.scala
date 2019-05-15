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

package za.co.absa.enceladus.menas.utils.converters.model0

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

/**
  * This is the object for deserializing Model 0 version of Enceladus Schema and convert it to Spark StructType
  */
object Serializer {
  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  implicit private val formatsJson: Formats = Serialization.formats(NoTypeHints).withBigDecimal

  /**
    * Converts a JSON in Model 0 format to a Spark StructType
    */
  def convertFromModel0ToStructType(json: String): StructType = convertToStructType(deserialize(json))

  /**
    * Deserializes a Model 0 JSON
    */
  def deserialize(json: String): Schema = {
    Serialization.read[Schema](json)
  }

  /**
    * Converts a Model 0 shema object to Spark StructType
    */
  def convertToStructType(schema: Schema): StructType = {
    StructType(schema.fields.map({ menas =>
      convertMenasToSparkField(menas)
    }))
  }

  /**
    * Converts a Mode 0 struct field to a Spark's StructField
    */
  private def convertMenasToSparkField(menasField: SchemaField): StructField = {
    val outStream = new ByteArrayOutputStream()
    objectMapper.writeValue(outStream, menasField.metadata)

    val metadata = Metadata.fromJson(new String(outStream.toByteArray(), "UTF-8"))

    StructField(
      name = menasField.name,
      dataType = convertMenasToSparkDataType(menasField),
      nullable = menasField.nullable,
      metadata = metadata)
   }

  /**
    * Converts a Mode 0 data type to a Spark's data type
    */
  private def convertMenasToSparkDataType(menasField: SchemaField): DataType = {
    menasField.`type` match {
      case "array"  => convertMenasToSparkArray(menasField)
      case "struct" => StructType(convertMenasToSparkFields(menasField.children))
      case s        => CatalystSqlParser.parseDataType(s)
    }
  }

  /**
    * Converts a Mode 0 array field to a Spark's ArrayType field
    */
  private def convertMenasToSparkArray(arrayField: SchemaField): ArrayType = {
    if (arrayField.`type` != "array") {
      throw new IllegalStateException(s"An array is expected.")
    }
    arrayField.elementType match {
      case Some("struct") => ArrayType(StructType(convertMenasToSparkFields(arrayField.children)))
      case Some("array") => ArrayType(convertMenasToSparkArray(arrayField.children.head))
      case Some(primitive) => ArrayType(CatalystSqlParser.parseDataType(primitive))
      case None =>
        val fieldName = s"${arrayField.name}"
        throw new IllegalStateException(s"Element type is not specified for $fieldName.")
    }
  }

  /**
    * Converts a seq of Menas schema fields onto the spark structFields
    */
  private def convertMenasToSparkFields(menasFields: Seq[SchemaField]): Seq[StructField] = {
    menasFields.map({ menas =>
      convertMenasToSparkField(menas)
    })
  }

}
