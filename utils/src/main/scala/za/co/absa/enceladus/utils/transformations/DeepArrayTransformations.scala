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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.collection.mutable.ListBuffer

object DeepArrayTransformations {
  /**
    * Map transformation for columns that can be inside nested structs, arrays and its combinations.
    *
    * If the input column is a primitive field the method will add outputColumnName at the same level of nesting.
    * By executing the `expression` passing the source column into it. If a struct column is expected you can
    * use `.getField(...)` method to operate on it's children.
    *
    * The output column name can omit the full path as the field will be created at the same level of nesting as the input column.
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedWithColumnMap(df: DataFrame,
                          inputColumnName: String,
                          outputColumnName: String,
                          expression: Column => Column): DataFrame = {
    nestedWithColumnMapHelper(df, inputColumnName, outputColumnName, expression, null)._1
  }

  /**
    * A nested map that also appends errors to the error column (see [[DeepArrayTransformations.nestedWithColumnMap]] above)
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @param errorCondition   A function that takes an input column and returns an expression for an error column.
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedWithColumnAndErrorMap(df: DataFrame,
                                  inputColumnName: String,
                                  outputColumnName: String,
                                  errorColumnName: String,
                                  expression: Column => Column,
                                  errorCondition: Column => Column
                                 ): DataFrame = {

    if (errorColumnName.contains('.')) {
      throw new IllegalArgumentException(s"Error columns should be at the root schema level. Value '$errorColumnName' is not valid.")
    }

    val (dfOut: DataFrame, deepErrorColumn: String) = nestedWithColumnMapHelper(df, inputColumnName, outputColumnName, expression, errorCondition)

    gatherErrors(dfOut, deepErrorColumn, errorColumnName)
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
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedStructMap(df: DataFrame,
                      inputStructField: String,
                      outputChildField: String,
                      expression: Column => Column
                     ): DataFrame = {
    val updatedStructField = if (inputStructField.nonEmpty) inputStructField + ".*" else ""
    nestedWithColumnMap(df, updatedStructField, outputChildField, expression)
  }

  /**
    * Add a column that can be inside nested structs, arrays and its combinations
    *
    * @param df            Dataframe to be transformed
    * @param newColumnName A column name to be created
    * @param expression    A function that returns the value of the new column as a Spark expression
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedAddColumn(df: DataFrame,
                      newColumnName: String,
                      expression: Unit => Column): DataFrame = {
    nestedWithColumnMap(df, newColumnName, "", c => expression())
  }

  /**
    * Drop a column from inside a nested structs, arrays and its combinations
    *
    * @param df           Dataframe to be transformed
    * @param columnToDrop A column name to be dropped
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedDropColumn(df: DataFrame,
                       columnToDrop: String): DataFrame = {
    nestedWithColumnMap(df, columnToDrop, "", null)
  }

  /**
    * This is a helper function for all mapping transformations.
    *
    * It combines many operations inside one traversal. It is used to
    * - Map a column
    * - Map a column with an additional creation of an error column
    * - Add/remove column
    *
    * It is probably too complicated to use by itself. Use facade functions instead.
    *
    * If the input column is a primitive field the method will add outputColumnName at the same level of nesting.
    * By executing the `expression` passing the source column into it. If a struct column is expected you can
    * use `.getField(...)` method to operate on it's children.
    *
    * The output column name can omit the full path as the field will be created at the same level of nesting as the input column.
    *
    * If the input column ends with '*', e.g. `shop.manager.*`, the struct itself will be passed as the lambda parameter,
    * but the new column will be placed inside the struct. This behavior is used in [[DeepArrayTransformations.nestedStructMap]].
    *
    * If the input column does not exist, the column will be created passing null as a column parameter to the expression.
    * This behavior is used in [[DeepArrayTransformations.nestedAddColumn]].
    *
    * If null is passed as an expression the input column will be dropped. This behavior is used in
    * [[DeepArrayTransformations.nestedDropColumn]].
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @return A pair consisting of a dataframe with a new field that contains transformed values and a string containing the error column name.
    */
  private def nestedWithColumnMapHelper(df: DataFrame,
                                        inputColumnName: String,
                                        outputColumnName: String,
                                        expression: Column => Column,
                                        errorCondition: Column => Column
                                       ): (DataFrame, String) = {
    // The name of the field is the last token of outputColumnName
    val outputFieldName = outputColumnName.split('.').last
    var errorColumnName = ""

    // Sequential lambda name generator
    var lambdaIndex = 1

    def getLambdaName: String = {
      val name = s"v$lambdaIndex"
      lambdaIndex += 1
      name
    }

    def addErrorColumn(schema: Option[StructType], column: Column): Option[Column] = {
      if (errorCondition == null) {
        None
      } else {
        errorColumnName = SchemaUtils.getUniqueName( "errorList", schema)
        val errorColumn = array(errorCondition(column)).as(errorColumnName)
        if (inputColumnName.contains('.')) {
          val parent = inputColumnName.split('.').dropRight(1).mkString(".")
          errorColumnName = s"$parent.$errorColumnName"
        }
        Some(errorColumn)
      }
    }

    // Handle the case when the input column is inside a nested struct
    def mapStruct(schema: StructType, path: Seq[String], parentColumn: Option[Column] = None): Seq[Column] = {
      val fieldName = path.head
      val isLeaf = isLeafElement(path)
      val mappedFields = new ListBuffer[Column]()
      var fieldFound = false

      val newColumns = schema.fields.flatMap(field => {
        // This is the original column (struct field) we want to process
        val curColumn = parentColumn match {
          case None => new Column(field.name)
          case Some(col) => col.getField(field.name).as(field.name)
        }

        if (field.name.compareToIgnoreCase(fieldName) != 0) {
          // Copy unrelated fields as they were
          Seq(curColumn)
        } else {
          // We have found a match
          fieldFound = true
          if (isLeaf) {
            if (expression == null) {
              // Drops the column if the expression is null
              Nil
            } else {
              field.dataType match {
                case dt: ArrayType =>
                  mapArray(dt, path, parentColumn)
                case _ =>
                  mappedFields += expression(curColumn).as(outputFieldName)
                  addErrorColumn(Some(schema), curColumn).foreach(mappedFields += _)
                  Seq(curColumn)
              }
            }
          } else {
            // Non-leaf columns need to be further processed recursively
            field.dataType match {
              case dt: StructType => Seq(struct(mapStruct(dt, path.tail, Some(curColumn)): _*).as(fieldName))
              case dt: ArrayType => mapArray(dt, path, parentColumn)
              case _ => throw new IllegalArgumentException(s"Field ${field.name} is not a struct type or an array.")
            }
          }

        }
      })

      if (isLeaf && !fieldFound) {
        if (fieldName == "*") {
          // If a star is specified as the last field name => manipulation on a struct itself is requested
          val parentField = parentColumn.orNull
          mappedFields += expression(parentField).as(outputFieldName)
          addErrorColumn(Some(schema), parentField).foreach(mappedFields += _)
        } else {
          // Field not found => an addition of a new field is requested
          val fieldToAdd = if (fieldName.isEmpty) outputFieldName else fieldName
          mappedFields += expression(null).as(fieldToAdd)
        }
      }
      newColumns ++ mappedFields
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
    def mapNestedArrayOfErrors(schema: ArrayType, curColumn: Column): Column = {
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
      val isLeaf = isLeafElement(path)
      val elemType = schema.elementType
      val lambdaName = getLambdaName
      val fieldName = path.head
      val mappedFields = new ListBuffer[Column]()

      val curColumn = parentColumn match {
        case None => new Column(fieldName)
        case Some(col) if !isParentArray => col.getField(fieldName).as(fieldName)
        case Some(col) if isParentArray => col
      }

      // Handles primitive data types as well as nested arrays of primitives
      def handlePrimitive(dt: DataType, transformExpression: Column, errorExpression: Column) = {
        if (isLeaf) {
          if (expression != null) {
            // Retain the original column
            mappedFields += transform(curColumn, lambdaName, transformExpression).as(outputFieldName)

            // Handle error column for arrays of primitives
            if (errorExpression != null) {
              errorColumnName = SchemaUtils.getUniqueName( "errorList", None)
              val errorColumn = transform(curColumn, lambdaName, errorExpression).as(errorColumnName)
              if (inputColumnName.contains('.')) {
                val parent = inputColumnName.split('.').dropRight(1).mkString(".")
                errorColumnName = s"$parent.$errorColumnName"
              }
              mappedFields += errorColumn
            }
            curColumn
          } else {
            // Drops it otherwise
            null
          }
        } else {
          // This is the case then the caller requested to map a field that is a child of a primitive.
          // For instance, the caller is requested to map 'person.firstName.foo' when 'person.firstName'
          // is an instance of StringType.
          throw new IllegalArgumentException(s"Field $fieldName is not a struct or an array of struct type.")
        }
      }

      val newColumn = elemType match {
        case dt: StructType =>
          // If the leaf array element is struct we need to create the output field inside the struct itself.
          // This is done by specifying "*" as a leaf field.
          // If this struct is not a leaf element we just recursively call mapStruct() with child portion of the path.
          val innerPath = if (isLeaf) Seq("*") else path.tail
          val innerStruct = struct(mapStruct(dt, innerPath, Some(_$(lambdaName))): _*)
          transform(curColumn, lambdaName, innerStruct).as(fieldName)
        case dt: ArrayType =>
          // This is the case when the input field is a several nested arrays of arrays of...
          // Each level of array nesting needs to be dealt with using transform()
          val deepestType = SchemaUtils.getDeepestArrayType(dt)
          deepestType match {
            case _: StructType =>
              // If at the bottom of the array nesting is a struct we need to add the output column
              // as a field of that struct
              // Example: if 'persons' is an array of array of structs having firstName and lastName,
              //          fields, then 'conformedFirstName' needs to be a new field inside the struct
              val innerArray = mapArray(dt, path, Some(_$(lambdaName)), isParentArray = true)
              transform(curColumn, lambdaName, innerArray.head).as(fieldName)
            case _ =>
              // If at the bottom of the array nesting is a primitive we need to add the new column
              // as an array of it's own
              // Example: if 'persons' is an array of array of string the output field,
              //          say, 'conformedPersons' needs also to be an array of array of string.
              handlePrimitive(dt, mapNestedArrayOfPrimitives(dt, _$(lambdaName)), null)
          }
        case dt =>
          // This handles an array of primitives, e.g. arrays of strings etc.
          val errorExpression = if (errorCondition == null) null else errorCondition(_$(lambdaName))
          handlePrimitive(dt, expression(_$(lambdaName)), errorExpression)
      }
      if (newColumn == null) {
        mappedFields
      } else {
        Seq(newColumn) ++ mappedFields
      }
    }

    val schema = df.schema
    val path = inputColumnName.split('.')
    (df.select(mapStruct(schema, path): _*), errorColumnName) // ;-]
  }

  /**
    * Gathers errors from a nested error column into a global error column for the dataframe
    *
    * @param nestedErrorColumn A column name that can be nested deeply inside the dataframe.
    * @param globalErrorColumn An error column name at the root shema level.
    * @return A dataframe with a new field that contains the list of errors.
    */
  def gatherErrors(df: DataFrame,
                   nestedErrorColumn: String,
                   globalErrorColumn: String): DataFrame = {

    def flattenNestedArrays(schema: StructType, inputColumn: String): Column = {
      def helperStruct(schema: StructType, columnPath: Seq[String], inputColumn: Column): Column = {
        if (isLeafElement(columnPath)) {
          inputColumn
        } else {
          schema.apply(columnPath.head).dataType match {
            case st: StructType => helperStruct(st, columnPath.tail, inputColumn)
            case ar: ArrayType => helperArray(ar, columnPath, inputColumn)
            case _ => inputColumn
          }
        }
      }

      def helperArray(arr: ArrayType, columnPath: Seq[String], inputColumn: Column): Column = {
        arr.elementType match {
          case st: StructType =>
            if (columnPath.isEmpty) {
              flatten(inputColumn)
            } else {
              flatten(helperStruct(st, columnPath.tail, inputColumn))
            }
          case ar: ArrayType =>
            flatten(helperArray(ar, columnPath, inputColumn))
          case _ =>
            flatten(inputColumn)
        }
      }

      val path = nestedErrorColumn.split('.')
      helperStruct(df.schema, path, col(inputColumn))
    }

    if (globalErrorColumn.contains('.')) {
      throw new IllegalArgumentException(s"Global error columns should be at the root schema level. Value '$globalErrorColumn' is not valid.")
    }

    //df.printSchema()
    //df.explain(true)

    val tmpCol = SchemaUtils.getUniqueName("tmp", Some(df.schema))
    val flattenedColumn = flattenNestedArrays(df.schema, nestedErrorColumn)

    val df1 = df.withColumnRenamed(globalErrorColumn,tmpCol)
      .withColumn(globalErrorColumn, callUDF( "arrayDistinctErrors", concat(col(tmpCol), flattenedColumn)))
      .drop(col(tmpCol))

    //df1.printSchema()
    //df1.explain(true)


    nestedDropColumn(df1, nestedErrorColumn)
  }

  // Returns true if a path consists only of 1 element meaning it is the leaf element of the input column path requested by the caller
  private def isLeafElement(path: Seq[String]): Boolean = path.lengthCompare(2) < 0 // More efficient version of path.length == 1
}
