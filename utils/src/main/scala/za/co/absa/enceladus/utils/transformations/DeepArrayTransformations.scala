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

package za.co.absa.enceladus.utils.transformations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{_$, struct, transform}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import scala.collection.mutable.ListBuffer

object DeepArrayTransformations {
  /**
    * Map transformation for columns that can be inside nested structs, arrays and it's combinations.
    *
    * If the input column is a primitive field the method will add outputColumnName at the same level of nestness.
    * By executing the `expression` passing the source column into it. If a struct column is expected you can
    * use `.getField(...)` method to operate on it's children.
    *
    * The output column name can omit the full path as the field will be created at the same level of nestness as the input column.
    *
    * If the input column ends with '*', e.g. `shop.manager.*`, the struct itself will be passed as the lambda parameter,
    * but the new column will be placed inside the struct. This behavior is used in [[DeepArrayTransformations.nestedStructMap]].
    *
    * If the input column does not exist, the column will be creating passing null as a coulmn parameter to the expression.
    * This behavior is used in [[DeepArrayTransformations.nestedAddColumn]].
    *
    * If null is passed as an expression the input column will be dropped. This behavior is used in
    * [[DeepArrayTransformations.nestedDropColumn]].
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @return                 A dataframe with a new field that contains transformed values.
    */
  def nestedWithColumnMap(df: DataFrame,
                          inputColumnName: String,
                          outputColumnName: String,
                          expression: Column => Column): DataFrame = {
    // The name of the field is the last token of fieldOut
    val outputFieldName = outputColumnName.split('.').last

    // Sequential lambda name generator
    var lambdaIndex = 1

    def getLambdaName: String = {
      val name = s"v$lambdaIndex"
      lambdaIndex += 1
      name
    }

    // Handle the case when the input column is inside a nested struct
    def mapStruct(schema: StructType, path: Seq[String], parentColumn: Option[Column] = None): Seq[Column] = {
      val fieldName = path.head
      val isTop = path.lengthCompare(2) < 0
      val mappedFields = new ListBuffer[Column]()
      var fieldFound = false
      val newColumns = schema.fields.flatMap(field => {
        val curColumn = parentColumn match {
          case None => new Column(field.name)
          case Some(col) => col.getField(field.name).as(field.name)
        }

        if (field.name.compareToIgnoreCase(fieldName) != 0) {
          Seq(curColumn)
        } else {
          if (isTop) {
            field.dataType match {
              case dt: ArrayType =>
                fieldFound = true
                if (expression != null) {
                  mapArray(dt, path, parentColumn)
                } else {
                  Nil
                }
              case _ =>
                fieldFound = true
                if (expression != null) {
                  // Retain the original column
                  mappedFields += expression(curColumn).as(outputFieldName)
                  Seq(curColumn)
                } else {
                  // Drops it otherwise
                  Nil
                }
            }
          } else {
            field.dataType match {
              case dt: StructType => Seq(struct(mapStruct(dt, path.tail, Some(curColumn)): _*).as(fieldName))
              case dt: ArrayType => mapArray(dt, path, parentColumn)
              case _ => throw new IllegalArgumentException(s"Field ${field.name} is not a struct type or an array.")
            }
          }

        }
      })

      if (isTop && !fieldFound) {
        if (fieldName == "*") {
          // If a star is specified as the last field name => manipulation on a struct itself is requested
          val parentField = parentColumn.orNull
          mappedFields += expression(parentField).as(outputFieldName)
        } else {
          // Field not found => an addition of a new field is requested
          val fieldToAdd = if (fieldName.isEmpty) outputFieldName else fieldName
          mappedFields += expression(null).as(fieldToAdd)
        }
      }
      newColumns ++ mappedFields
    }

    // For an array of arrays of arrays, ... get the final element type at the bottom of the array
    def getDeepestArrayType(arrayType: ArrayType): DataType = {
      arrayType.elementType match {
        case a: ArrayType => getDeepestArrayType(a)
        case b => b
      }
    }

    // Handle arrays (including arrays of arrays) of primitives
    // The output column will also be an array, not an additional element of the existing array
    def mapNestedArrayOfPrimitives(schema: ArrayType, curColumn: Column): Column = {
      val lambdaName = getLambdaName
      val elemType = schema.elementType

      elemType match {
        case _: StructType => throw new IllegalArgumentException(s"Unexpected usage of mapNestedArrayOfPrimitives() on structs.")
        case dt: ArrayType =>
          val innerArray = mapNestedArrayOfPrimitives(dt, _$(lambdaName))
          transform(curColumn, lambdaName, innerArray)
        case dt => transform(curColumn, lambdaName, expression(_$(lambdaName)))
      }
    }

    // Handle the case when the input column is inside a nested array
    def mapArray(schema: ArrayType, path: Seq[String], parentColumn: Option[Column] = None, isParentArray: Boolean = false): Seq[Column] = {
      val isTop = path.lengthCompare(2) < 0
      val elemType = schema.elementType
      val lambdaName = getLambdaName
      val fieldName = path.head
      val mappedFields = new ListBuffer[Column]()

      val curColumn = parentColumn match {
        case None => new Column(fieldName)
        case Some(col) if !isParentArray => col.getField(fieldName).as(fieldName)
        case Some(col) if isParentArray => col
      }

      val newColumn = elemType match {
        case dt: StructType =>
          val innerStruct = struct(mapStruct(dt, path.tail, Some(_$(lambdaName))): _*)
          transform(curColumn, lambdaName, innerStruct).as(fieldName)
        case dt: ArrayType =>
          val deepestType = getDeepestArrayType(dt)
          deepestType match {
            case _: StructType =>
              val innerArray = mapArray(dt, path, Some(_$(lambdaName)), isParentArray = true)
              transform(curColumn, lambdaName, innerArray.head).as(fieldName)
            case _ =>
              if (isTop) {
                if (expression != null) {
                  // Retain the original column
                  mappedFields += transform(curColumn, lambdaName, mapNestedArrayOfPrimitives(dt, _$(lambdaName))).as(outputFieldName)
                  curColumn
                } else {
                  // Drops it otherwise
                  null
                }
              } else {
                throw new IllegalArgumentException(s"Field $fieldName is not a struct or an array of struct type.")
              }
          }
        case dt =>
          if (isTop) {
            if (expression != null) {
              // Retain the original column
              mappedFields += transform(curColumn, lambdaName, expression(_$(lambdaName))).as(outputFieldName)
              curColumn
            } else {
              // Drops it otherwise
              null
            }
          } else {
            throw new IllegalArgumentException(s"Field $fieldName is not a struct type or an array.")
          }
      }
      if (newColumn == null) {
        mappedFields
      } else {
        Seq(newColumn) ++ mappedFields
      }
    }

    val schema = df.schema
    val path = inputColumnName.split('.')
    df.select(mapStruct(schema, path): _*) // ;-]
  }

  /**
    * Add a column that can be inside nested structs, arrays and it's combinations
    *
    * @param df            Dataframe to be transformed
    * @param newColumnName A column name to be created
    * @param expression    A function that returns the value of the new column as a Spark expression
    * @return              A dataframe with a new field that contains transformed values.
    */
  def nestedAddColumn(df: DataFrame,
                      newColumnName: String,
                      expression: Unit => Column): DataFrame = {
    nestedWithColumnMap(df, newColumnName, "", c => expression())
  }

  /**
    * Drop a column from inside a nested structs, arrays and it's combinations
    *
    * @param df           Dataframe to be transformed
    * @param columnToDrop A column name to be dropped
    * @return             A dataframe with a new field that contains transformed values.
    */
  def nestedDropColumn(df: DataFrame,
                       columnToDrop: String): DataFrame = {
    nestedWithColumnMap(df, columnToDrop, "", null)
  }

  /**
    * A nested struct map. Given a struct field the method will create a new child field of that struct as a
    * transformation of struct fields. This is useful for transformations such as concatenation of fields.
    *
    * To use root of the schema as the input struct pass "" as the `inputStructField`.
    * In this case `null` will be passed to the lambda function.
    *
    * Here is an example demonstrating how to handle both root and nested cases:
    *
    * {{{
    * val dfOut = nestedWithColumnMap(df, columnPath, "combinedField", c => {
    * if (c==null) {
    *   // The columns are at the root level
    *   concat(col("city"), col("street"))
    * } else {
    *   // The columns are inside nested structs/arrays
    *   concat(c.getField("city"), c.getField("street"))
    * }
    * })
    * }}}
    *
    * @param inputStructField A struct column name for which to apply the transformation
    * @param outputChildField The output column name that will be added as a child of the source struct.
    * @param expression       A function that applies a transformation to a column as a Spark expression
    * @return                 A dataframe with a new field that contains transformed values.
    */
  def nestedStructMap(df: DataFrame,
                      inputStructField: String,
                      outputChildField: String,
                      expression: Column => Column
                     ): DataFrame = {
    val updatedStructField = if (inputStructField.nonEmpty) inputStructField + ".*" else ""
    nestedWithColumnMap(df, updatedStructField, outputChildField, expression)
  }
}
