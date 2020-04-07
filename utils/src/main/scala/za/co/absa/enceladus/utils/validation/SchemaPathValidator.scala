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

package za.co.absa.enceladus.utils.validation

import org.apache.spark.sql.types._

import scala.annotation.tailrec

/**
  * Object responsible for validating paths to fields, it's existence and case sensitivity
  */
object SchemaPathValidator {

  /**
    * Validate path existence
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.name")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPath(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathArray(schema, fieldPath.split('.'))
  }

  /**
    * Validate path for an output field.
    * The parent path must exist and be a struct. The full path should not exist.
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.name")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPathOutput(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathArray(schema, fieldPath.split('.'), parentOnly = true, fullPathNew = true)
  }

  /**
    * Validate parent path existence (e.g. for data.field.value the path data.field must exist)
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.name")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPathParent(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathArray(schema, fieldPath.split('.'), parentOnly = true)
  }


  /**
    * Validate two paths have the same parent
    *
    * For example, `structFoo.field1` and `structFoo.field2` have the same parent,
    * while `structFoo.field1` and `structBar.field2` have different parents,
    *
    * @param fieldPath1  A path to the first field (e.g. "data.employees.employee.name2")
    * @param fieldPath2  A path to the second field (e.g. "data.employees.employee.name2")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validatePathSameParent(fieldPath1: String, fieldPath2: String): Seq[ValidationIssue] = {
    val path1 = fieldPath1.split('.')
    val path2 = fieldPath2.split('.')

    if (path1.length == path2.length && path1.dropRight(1).sameElements(path2.dropRight(1))) {
      Seq.empty[ValidationIssue]
    } else {
      Seq(ValidationError(s"Fields '$fieldPath1' and '$fieldPath2' have different parents."))
    }
  }

  /**
    * Validate schema path data type is primitive
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.name")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPathPrimitive(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathType(schema, fieldPath) {
      case _: NumericType | _: StringType | _: BooleanType | _: DateType | _: TimestampType => Seq.empty[ValidationIssue]
      case k => Seq(ValidationError(s"The datatype '${k.typeName}' of '$fieldPath' field is not a primitive type"))
    }
  }

  /**
    * Validate schema path data type is numeric
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.age")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPathNumeric(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathType(schema, fieldPath) {
      case _: NumericType => Seq.empty[ValidationIssue]
      case k => Seq(ValidationError(s"The datatype '${k.typeName}' of '$fieldPath' field is not a numeric type"))
    }
  }

  /**
    * Validate schema path data type is algebraic
    *
    * @param schema      A Spark schema
    * @param fieldPath   A path to a field (e.g. "data.employees.employee.age")
    * @return A list of ValidationErrors objects, each containing a column name and the list of errors and warnings
    */
  def validateSchemaPathAlgebraic(schema: StructType, fieldPath: String): Seq[ValidationIssue] = {
    validateSchemaPathType(schema, fieldPath) {
      case _: NumericType |  _: BooleanType => Seq.empty[ValidationIssue]
      case k => Seq(ValidationError(s"The datatype '${k.typeName}' of '$fieldPath' field " +
        "is neither NumericType nor BooleanType"))
    }
  }

  private def validateSchemaPathType(schema: StructType, fieldPath: String)(f: DataType => Seq[ValidationIssue]): Seq[ValidationIssue] = {
    val path = fieldPath.split('.')
    if (path.isEmpty) {
      Seq(ValidationError(s"Column name is not specified"))
    } else {
      val optField = getSchemaField(schema, path)
      optField match {
        case None => Seq(ValidationError(s"Column path $fieldPath does not exists"))
        case Some(field) => f(field.dataType)
      }
    }
  }

  private def validateSchemaPathArray(schema: StructType,
                                      path: Array[String],
                                      parentOnly: Boolean = false,
                                      fullPathNew: Boolean = false,
                                      parentPath: String = ""): Seq[ValidationIssue] = {
    if (path.isEmpty) {
      Nil
    }
    else {

      val currentField = path(0)
      val fullPath = s"$parentPath${path.mkString(".")}"
      if (parentOnly && path.length == 1) {
        schema.fields match {
          case f if fullPathNew && f.exists(field => field.name == currentField) =>
            Seq(ValidationError(s"Column '$parentPath$currentField' already exists so it cannot be used as an output column '$fullPath'."))
          case f if fullPathNew && f.exists(field => field.name.compareToIgnoreCase(currentField) == 0) =>
            Seq(ValidationError(s"Case insensitive variant of a cloumn '$parentPath$currentField' already exists so it cannot be used as an output column '$fullPath'."))
          case _ => Nil
        }
      } else {

        val exactMatch = schema.fields.find(field => field.name == currentField)
        val failures = exactMatch match {
          case Some(field) =>
            val dataType = getUnderlyingType(field.dataType)
            dataType match {
              case st: StructType =>
                validateSchemaPathArray(st, path.drop(1), parentOnly, fullPathNew, s"$parentPath${field.name}.")
              case _ if path.length > 1 =>
                Seq(ValidationError(s"Column '$parentPath$currentField' is a primitive type and can't contain child fields '$fullPath'."))
              case _ => Nil
            }

          case None => caseInsensitiveErrors(schema, parentPath, currentField)
        }
        failures
      }
    }

  }

  def caseInsensitiveErrors(schema: StructType, parentPath: String, currentField: String): Seq[ValidationError] = {
    val caseInsensitiveMatch = schema.fields.find(field => field.name.compareToIgnoreCase(currentField) == 0)
    caseInsensitiveMatch match {
      case Some(field) =>
        Seq(ValidationError(s"Column name '$parentPath$currentField' does not case-sensitively match '$parentPath${field.name}'."))
      case None =>
        Seq(ValidationError(s"Column name '$parentPath$currentField' does not exist."))
    }
  }

  /** Returns underlying data type of a field after traversing nested arrays. */
  private def getUnderlyingType(dt: DataType): DataType = {
    val underlyingType = dt match {
      case arr: ArrayType => getUnderlyingType(arr.elementType)
      case a => a
    }
    underlyingType
  }

  @tailrec
  private def getSchemaField(schema: StructType, path: Array[String]): Option[StructField] = {
    if (path.isEmpty) {
      None
    } else {
      val field = schema.find(p => p.name == path.head)
      field match {
        case None => None
        case Some(fieldSchema) =>
          if (path.length > 1) {
            val dataType = getUnderlyingType(fieldSchema.dataType)
            dataType match {
              case st: StructType => getSchemaField(st, path.drop(1))
              case _ => None
            }
          } else {
            Some(fieldSchema)
          }
      }
    }
  }
}
