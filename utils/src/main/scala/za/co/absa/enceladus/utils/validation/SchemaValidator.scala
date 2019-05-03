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

package za.co.absa.enceladus.utils.validation

import org.apache.spark.sql.types._
import za.co.absa.enceladus.utils.validation.field.{FieldValidationFailure, FieldValidator}

import scala.collection.mutable.ListBuffer

/**
  * Object responsible for Spark schema validation against self inconsistencies (not against the actual data)
  */
object SchemaValidator {
  /**
    * Validate a schema
    *
    * @param schema A Spark schema
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchema(schema: StructType): Seq[FieldValidationFailure] = {
    var errorsAccumulator = new ListBuffer[FieldValidationFailure]
    val flatSchema = flattenSchema(schema)
    for {s <- flatSchema} {
      val fieldWithPath = if (s.structPath.isEmpty) s.field else s.field.copy(name = s.structPath + "." + s.field.name)
      val issues = validateColumnName(s.field.name, s.structPath) ++ FieldValidator.validate(fieldWithPath)
      if (issues.nonEmpty) {
        val pattern = if (s.field.metadata contains "pattern") s.field.metadata.getString("pattern") else ""
        errorsAccumulator += FieldValidationFailure(fieldWithPath.name, pattern, issues)
      }
    }
    errorsAccumulator.toList
  }

  /**
    * Validate a column name, check for illegal characters.
    * Currently it checks for dots only, but it is extendable.
    *
    * @param columnName A column name
    * @param structPath A path to the column name inside the nested structures
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  private def validateColumnName(columnName: String, structPath: String = "") : Seq[ValidationIssue] = {
    if (columnName contains '.') {
      val structMsg = if (structPath.isEmpty) "" else s" of the struct '$structPath'"
      Seq(ValidationError(s"Column name '$columnName'$structMsg contains an illegal character: '.'"))
    }
    else {
      Nil
    }
  }

  /**
    * This method flattens an input schema to a list of columns and their types
    * Struct types are collapsed as 'column.element' and arrays as 'column[].element', arrays as 'column[][].element'.
    *
    * @param schema A Spark schema
    * @return A sequence of all fields as a StructField
    */
  private def flattenSchema(schema: StructType): Seq[FlatField] = {

    def flattenStruct(schema: StructType, structPath: String): Seq[FlatField] = {
      var fields = new ListBuffer[FlatField]
      val prefix = if (structPath.isEmpty) structPath else structPath + "."
      for (field <- schema) {
        field.dataType match {
          case s: StructType => fields ++= flattenStruct(s, prefix + field.name)
          case a: ArrayType => fields ++= flattenArray(field, a, prefix + field.name + "[]")
          case _ =>
            val prefixedField = FlatField(structPath, field)
            fields += prefixedField
        }
      }
      fields
    }

    def flattenArray(field: StructField, arr: ArrayType, structPath: String): Seq[FlatField] = {
      var arrayFields = new ListBuffer[FlatField]
      arr.elementType match {
        case stuctInArray: StructType => arrayFields ++= flattenStruct(stuctInArray, structPath)
        case arrayType: ArrayType => arrayFields ++= flattenArray(field, arrayType, structPath + "[]")
        case _ =>
          val prefixedField = FlatField(structPath, field)
          arrayFields += prefixedField
      }
      arrayFields
    }

    flattenStruct(schema, "")
  }

}
