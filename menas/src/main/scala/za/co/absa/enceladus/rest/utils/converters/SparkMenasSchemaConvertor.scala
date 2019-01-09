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

package za.co.absa.enceladus.rest.utils.converters

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream

@Component
class SparkMenasSchemaConvertor @Autowired() (val objMapper: ObjectMapper) {

  /**
   * Converts a seq of menas schema fields onto the spark structfields
   */
  def convertMenasToSparkFields(menasFields: Seq[SchemaField]): Seq[StructField] = {
    menasFields.map({ menas =>
      menasToSparkField(menas)
    })
  }

  def convertSparkToMenasFields(sparkFields: Seq[StructField]): Seq[SchemaField] = {
    convertSparkToMenasFields(sparkFields, "");
  }

  /** Converts a seq of spark struct fields to menas representation */
  def convertSparkToMenasFields(sparkFields: Seq[StructField], path: String): Seq[SchemaField] = {
    sparkFields.map({ field =>
      val arr = if (field.dataType.isInstanceOf[ArrayType]) {
        Some(field.dataType.asInstanceOf[ArrayType])
      } else None

      SchemaField(
        name = field.name,
        `type` = field.dataType.typeName,
        path = path,
        elementType = arr.map(_.elementType.typeName),
        containsNull = arr.map(_.containsNull),
        nullable = field.nullable,
        metadata = objMapper.readValue(field.metadata.json, classOf[Map[String, String]]),
        children = getChildren(field.dataType, s"$path${if (path.isEmpty()) "" else "."}${field.name}"))
    }).toList
  }

  /** Calculate the children field for the spark to menas conversion */
  private def getChildren(spark: DataType, path: String): List[SchemaField] = {
    spark match {
      case s: StructType => convertSparkToMenasFields(s.fields, path).toList
      case a @ ArrayType(el: ArrayType, _) if a.elementType.isInstanceOf[ArrayType] => List(SchemaField(name = "", `type` = el.typeName, path = path, elementType = Some(el.elementType.typeName),
        containsNull = Some(el.containsNull), nullable = a.containsNull, metadata = Map(), children = getChildren(el, path)))
      case a: ArrayType => getChildren(a.elementType, path)
      case m: MapType   => getChildren(m.valueType, path)
      case _: DataType  => List()
    }
  }

  private def menasToSparkField(menasField: SchemaField): StructField = {

    val outStream = new ByteArrayOutputStream()
    objMapper.writeValue(outStream, menasField.metadata)

    val metadata = Metadata.fromJson(new String(outStream.toByteArray(), "UTF-8"))

    StructField(
      name = menasField.name,
      dataType = getSparkDataType(menasField),
      nullable = menasField.nullable,
      metadata = metadata)
  }

  private def getSparkDataType(menasField: SchemaField): DataType = {
    menasField.`type` match {
      case "array"  => ArrayType.apply(getSparkDataType(menasField.children.head), menasField.containsNull.get)
      case "struct" => StructType(convertMenasToSparkFields(menasField.children))
      case s        => CatalystSqlParser.parseDataType(s)
    }
  }

}
