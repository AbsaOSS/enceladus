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

package za.co.absa.enceladus.menas.utils.converters

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream

import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.util.control.NonFatal

@Component
class SparkMenasSchemaConvertor @Autowired()(val objMapper: ObjectMapper) {

  /**
    * Converts a JSON of any supported format into a Spark StructType
    */
  def convertAnyToStructType(inputJson: String): StructType = {
    // This looks like a monadic structure. I wonder if this code can be simplified.
    convertStructTypeJsonToStructType(inputJson) match {
      case Left(schema) => schema
      case Right(message1) =>
        throw new IllegalStateException(
          s"Unable to parse schema JSON\nStructType serializer: $message1\nMenas serializer: $message1")

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
    * Converts a seq of menas schema fields onto the spark structfields
    */
  def convertMenasToSparkFields(menasFields: Seq[SchemaField]): Seq[StructField] = {
    menasFields.map({ menas =>
      menasToSparkField(menas)
    })
  }

  /**
    * Converts a menas array to a spark array
    */
  def convertMenasToSparkArray(arrayField: SchemaField): ArrayType = {
    if (arrayField.`type` != "array") {
      throw new IllegalStateException(s"An array is expected.")
    }
    arrayField.elementType match {
      case Some("struct") => ArrayType(StructType(convertMenasToSparkFields(arrayField.children)))
      case Some("array") => ArrayType(convertMenasToSparkArray(arrayField.children.head))
      case Some("map") => ArrayType(convertMenasToSparkMap(arrayField.children.head))
      case Some(primitive) => ArrayType(CatalystSqlParser.parseDataType(primitive))
      case None =>
        val fieldName = s"${arrayField.path} ${arrayField.name}"
        throw new IllegalStateException(s"Element type is not specified for $fieldName.")
    }
  }

  /**
   * Converts Menas [[SchemaField]] to a [[MapType]] (key dataType = String)
   */
  def convertMenasToSparkMap(mapField: SchemaField): MapType = {
    if (mapField.`type` != "map") {
      throw new IllegalStateException(s"A map is expected.")
    }
    require(mapField.containsNull.isDefined, "containsNull must be defined for map")

    val valueContainsNull = mapField.containsNull.get

    // The content of the map is either encoded in elementType (just primaryType or struct, array, map).
    // For the container types, the children must be inspected
    mapField.elementType match {
      case Some("struct") => MapType(StringType, StructType(convertMenasToSparkFields(mapField.children)), valueContainsNull)
      case Some("array") => MapType(StringType, convertMenasToSparkArray(mapField.children.head), valueContainsNull)
      case Some("map") => MapType(StringType, convertMenasToSparkMap(mapField.children.head), valueContainsNull)
      case Some(primitive) => MapType(StringType, CatalystSqlParser.parseDataType(primitive), valueContainsNull)
      case None =>
        val fieldName = s"${mapField.path} ${mapField.name}"
        throw new IllegalStateException(s"Element type is not specified for $fieldName.")
    }
  }

  @throws[SchemaParsingException]
  def convertSparkToMenasFields(sparkFields: Seq[StructField]): Seq[SchemaField] = {
    convertSparkToMenasFields(sparkFields, "")
  }

  /** Converts a seq of spark struct fields to menas representation */
  def convertSparkToMenasFields(sparkFields: Seq[StructField], path: String): Seq[SchemaField] = {
    sparkFields.map(sparkToMenasField(_, path))
  }

  private def sparkToMenasField(field: StructField, path: String): SchemaField = {
    val containerInfo = field.dataType match {
      case arrayType: ArrayType => Some(arrayType.elementType.typeName, arrayType.containsNull)
      case mapType: MapType     =>
        if (mapType.keyType != StringType) {
          throw SchemaParsingException(
            schemaType = null, //scalastyle:ignore null - unknown value = null
            field = Option(field.name), message =
            s"Only StringType key is allowed for MapType, but found: ${mapType.keyType}"
          )
        }
        Some(mapType.valueType.typeName, mapType.valueContainsNull)
      case _                    => None
    }

    val metadata: Map[String, String] = objMapper.readValue(field.metadata.json, classOf[Map[Any, Any]])
        .collect{
          // objMapper.readValue(_, classOf[Map[String, String]] doesn't actually enforce typing within the Map, this
          // construction ensures it
          case (key: String, value: String) => (key, value)
          case (key: String, null) => (key, null) // scalastyle:ignore null - some values can be null, particularly default, can be null
          case (key, value) => throw SchemaParsingException(
            schemaType = null,  //scalastyle:ignore null - unknown value = null
            message = s"Value for metadata key '$key' (of value $value) to be a string or null",
            field = Option(field.name)
          )
        }

    val childrenPath = SchemaUtils.appendPath(path, field.name)

    SchemaField(
      name = field.name,
      `type` = field.dataType.typeName,
      path = path,
      elementType = containerInfo.map(_._1),
      containsNull = containerInfo.map(_._2),
      nullable = field.nullable,
      metadata = metadata,
      children = getChildren(field.dataType, childrenPath)
    )
  }

  /** Calculate the children field for the spark to menas conversion */
  private def getChildren(fieldDataType: DataType, path: String): List[SchemaField] = {
    def extractSubContainer(subDataType: DataType, containsNull: Boolean): List[SchemaField] = {
      subDataType match {
        case el:ArrayType => List(SchemaField(
          name = "",
          `type` = el.typeName,
          path = path,
          elementType = Some(el.elementType.typeName),
          containsNull = Some(el.containsNull),
          nullable = containsNull,
          metadata = Map(),
          children = getChildren(el, path)))
        case el:MapType => List(SchemaField(
          name = "",
          `type` = el.typeName,
          path = path,
          elementType = Some(el.valueType.typeName),
          containsNull = Some(el.valueContainsNull),
          nullable = containsNull,
          metadata = Map(),
          children = getChildren(el, path)))
      }
    }

    def isContainer(dataType: DataType): Boolean = {
      dataType.isInstanceOf[ArrayType] || dataType.isInstanceOf[MapType]
    }

    fieldDataType match {
      case s: StructType => convertSparkToMenasFields(s.fields, path).toList
      case a @ ArrayType(el, _) if isContainer(el) => extractSubContainer(el, a.containsNull)
      case a: ArrayType => getChildren(a.elementType, path)
      case m @ MapType(_, el, _) if isContainer(el) => extractSubContainer(el, m.valueContainsNull)
      case m: MapType => getChildren(m.valueType, path)
      case _: DataType => List()
    }
  }

  private def menasToSparkField(menasField: SchemaField): StructField = {

    val outStream = new ByteArrayOutputStream()
    objMapper.writeValue(outStream, menasField.metadata)

    val metadata = Metadata.fromJson(new String(outStream.toByteArray, "UTF-8"))

    StructField(
      name = menasField.name,
      dataType = getSparkDataType(menasField),
      nullable = menasField.nullable,
      metadata = metadata)
  }

  private def getSparkDataType(menasField: SchemaField): DataType = {
    menasField.`type` match {
      case "array"  => convertMenasToSparkArray(menasField)
      case "struct" => StructType(convertMenasToSparkFields(menasField.children))
      case "map"    => convertMenasToSparkMap(menasField)
      case s        => CatalystSqlParser.parseDataType(s)
    }
  }

}
