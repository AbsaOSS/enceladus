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

package za.co.absa.enceladus.rest_api.utils.converters

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream

import za.co.absa.spark.commons.utils.SchemaUtils
import za.co.absa.enceladus.rest_api.models.rest.exceptions.SchemaParsingException

import scala.util.control.NonFatal

@Component
class SparkEnceladusSchemaConvertor @Autowired()(val objMapper: ObjectMapper) {

  /**
    * Converts a JSON of any supported format into a Spark StructType
    */
  def convertAnyToStructType(inputJson: String): StructType = {
    // This looks like a monadic structure. I wonder if this code can be simplified.
    convertStructTypeJsonToStructType(inputJson) match {
      case Left(schema) => schema
      case Right(message1) =>
        convertEnceladusModel0JsonToStructType(inputJson) match {
          case Left(schema) => schema
          case Right(message2) =>
            throw new IllegalStateException(
              s"Unable to parse schema JSON\nStructType serializer: $message1\nEnceladus serializer: $message2")
        }
    }
  }

  /**
    * Converts a JSON in Spark's StructType format into an instance of Spark's StructType
    */
  def convertStructTypeJsonToStructType(inputJson: String): Either[StructType, String] = {
    try {
      val schema = DataType.fromJson(inputJson).asInstanceOf[StructType]
      Left(schema)
    }
    catch {
      case NonFatal(e) => Right(e.getMessage)
    }
  }

  /**
    * Converts a model 0 JSON (pre-release Enceladus) format into an instance of Spark's StructType
    */
  def convertEnceladusModel0JsonToStructType(inputJson: String): Either[StructType, String] = {
    try {
      val schema = model0.Serializer.convertToStructType(inputJson)
      Left(schema)
    }
    catch {
      case NonFatal(e) => Right(e.getMessage)
    }
  }

  /**
    * Converts a seq of Enceladus schema fields onto the spark structfields
    */
  def convertEnceladusToSparkFields(enceladusFields: Seq[SchemaField]): Seq[StructField] = {
    enceladusFields.map({ field =>
      enceladusToSparkField(field)
    })
  }

  /**
    * Converts a Enceladus array to a spark array
    */
  def convertEnceladusToSparkArray(arrayField: SchemaField): ArrayType = {
    if (arrayField.`type` != "array") {
      throw new IllegalStateException(s"An array is expected.")
    }
    arrayField.elementType match {
      case Some("struct") => ArrayType(StructType(convertEnceladusToSparkFields(arrayField.children)))
      case Some("array") => ArrayType(convertEnceladusToSparkArray(arrayField.children.head))
      case Some(primitive) => ArrayType(CatalystSqlParser.parseDataType(primitive))
      case None =>
        val fieldName = s"${arrayField.path} ${arrayField.name}"
        throw new IllegalStateException(s"Element type is not specified for $fieldName.")
    }
  }

  @throws[SchemaParsingException]
  def convertSparkToEnceladusFields(sparkFields: Seq[StructField]): Seq[SchemaField] = {
    convertSparkToEnceladusFields(sparkFields, "")
  }

  /** Converts a seq of spark struct fields to Enceladus representation */
  def convertSparkToEnceladusFields(sparkFields: Seq[StructField], path: String): Seq[SchemaField] = {
    sparkFields.map(sparkToEnceladusField(_, path))
  }

  private def sparkToEnceladusField(field: StructField, path: String): SchemaField = {
    val arr = field.dataType match {
      case arrayType: ArrayType => Some(arrayType)
      case _                    => None
    }

    val metadata: Map[String, String] = objMapper.readValue(field.metadata.json, classOf[Map[Any, Any]])
        .collect{
          // objMapper.readValue(_, classOf[Map[String, String]] doesn't actually enforce typing within the Map, this
          // construction ensures it
          case (key: String, value: String) => (key, value)
          case (key: String, value: Int) => (key, value.toString)
          case (key: String, null) => (key, null) // scalastyle:ignore null - some values can be null, particularly default, can be null
          case (key, value) => throw SchemaParsingException(
            schemaType = null,  //scalastyle:ignore null - unknown value = null
            message = s"Value for metadata key '$key' (of value $value) to be a string or int or null",
            field = Option(field.name)
          )
        }

    val childrenPath = SchemaUtils.appendPath(path, field.name)

    SchemaField(
      name = field.name,
      `type` = field.dataType.typeName,
      path = path,
      elementType = arr.map(_.elementType.typeName),
      containsNull = arr.map(_.containsNull),
      nullable = field.nullable,
      metadata = metadata,
      children = getChildren(field.dataType, childrenPath)
    )
  }

  /** Calculate the children field for the spark to Enceladus conversion */
  private def getChildren(fieldDataType: DataType, path: String): List[SchemaField] = {
    fieldDataType match {
      case s: StructType => convertSparkToEnceladusFields(s.fields, path).toList
      case a@ArrayType(el: ArrayType, _) => List(SchemaField(
        name = "",
        `type` = el.typeName,
        path = path,
        elementType = Some(el.elementType.typeName),
        containsNull = Some(el.containsNull),
        nullable = a.containsNull,
        metadata = Map(),
        children = getChildren(el, path)))
      case a: ArrayType => getChildren(a.elementType, path)
      case m: MapType => getChildren(m.valueType, path)
      case _: DataType => List()
    }
  }

  private def enceladusToSparkField(enceladusField: SchemaField): StructField = {

    val outStream = new ByteArrayOutputStream()
    objMapper.writeValue(outStream, enceladusField.metadata)

    val metadata = Metadata.fromJson(new String(outStream.toByteArray, "UTF-8"))

    StructField(
      name = enceladusField.name,
      dataType = getSparkDataType(enceladusField),
      nullable = enceladusField.nullable,
      metadata = metadata)
  }

  private def getSparkDataType(enceladusField: SchemaField): DataType = {
    enceladusField.`type` match {
      case "array"  => convertEnceladusToSparkArray(enceladusField)
      case "struct" => StructType(convertEnceladusToSparkFields(enceladusField.children))
      case s        => CatalystSqlParser.parseDataType(s)
    }
  }

}
