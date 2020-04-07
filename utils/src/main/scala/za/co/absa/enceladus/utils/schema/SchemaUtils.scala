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

package za.co.absa.enceladus.utils.schema

import org.apache.spark.sql.types._
import scala.annotation.tailrec
import scala.util.{Random, Try}

object SchemaUtils {

  /**
    * Get a field from a text path and a given schema
    * @param path   The dot-separated path to the field
    * @param schema The schema which should contain the specified path
    * @return       Some(the requested field) or None if the field does not exist
    */
  def getField(path: String, schema: StructType): Option[StructField] = {

    @tailrec
    def goThroughArrayDataType(dataType: DataType): DataType = {
      dataType match {
        case ArrayType(dt, _) => goThroughArrayDataType(dt)
        case result => result
      }
    }

    @tailrec
    def examineStructField(names: List[String], structField: StructField): Option[StructField] = {
      if (names.isEmpty) {
        Option(structField)
      } else {
        structField.dataType match {
          case struct: StructType         => examineStructField(names.tail, struct(names.head))
          case ArrayType(el: DataType, _) =>
            goThroughArrayDataType(el) match {
              case struct: StructType => examineStructField(names.tail, struct(names.head))
              case _                  => None
            }
          case _                          => None
        }
      }
    }

    val pathTokens = path.split('.').toList
    Try{
      examineStructField(pathTokens.tail, schema(pathTokens.head))
    }.getOrElse(None)
  }

  /**
    * Get a type of a field from a text path and a given schema
    *
    * @param path   The dot-separated path to the field
    * @param schema The schema which should contain the specified path
    * @return Some(the type of the field) or None if the field does not exist
    */
  def getFieldType(path: String, schema: StructType): Option[DataType] = {
    getField(path, schema).map(_.dataType)
  }

  /**
    * Checks if the specified path is an array of structs
    *
    * @param path   The dot-separated path to the field
    * @param schema The schema which should contain the specified path
    * @return true if the field is an array of structs
    */
  def isColumnArrayOfStruct(path: String, schema: StructType): Boolean = {
    getFieldType(path, schema) match {
      case Some(dt) =>
        dt match {
          case arrayType: ArrayType =>
            arrayType.elementType match {
              case _: StructType => true
              case _ => false
            }
          case _ => false
        }
      case None => false
    }
  }

  /**
    * Get nullability of a field from a text path and a given schema
    *
    * @param path   The dot-separated path to the field
    * @param schema The schema which should contain the specified path
    * @return Some(nullable) or None if the field does not exist
    */
  def getFieldNullability(path: String, schema: StructType): Option[Boolean] = {
    getField(path, schema).map(_.nullable)
  }

  /**
    * Checks if a field specified by a path and a schema exists
    * @param path   The dot-separated path to the field
    * @param schema The schema which should contain the specified path
    * @return       True if the field exists false otherwise
    */
  def fieldExists(path: String, schema: StructType): Boolean = {
    getField(path, schema).nonEmpty
  }

  /**
    * Returns all renames in the provided schema.
    * @param schema                       schema to examine
    * @param includeIfPredecessorChanged  if set to true, fields are included even if their name have not changed but
    *                                     a predecessor's (parent, grandparent etc.) has
    * @return        the keys of the returned map are the columns' names after renames, the values are the source columns;
    *                the name are full paths denoted with dot notation
    */
  def getRenamesInSchema(schema: StructType, includeIfPredecessorChanged: Boolean = true): Map[String, String] = {

    def getRenamesRecursively(path: String, sourcePath: String, struct: StructType, renamesAcc: Map[String, String], predecessorChanged: Boolean): Map[String, String] = {
      import za.co.absa.enceladus.utils.implicits.StructFieldImplicits.StructFieldEnhancements

      struct.fields.foldLeft(renamesAcc) { (renamesSoFar, field) =>
        val fieldFullName = appendPath(path, field.name)
        val fieldSourceName = field.getMetadataString(MetadataKeys.SourceColumn).getOrElse(field.name)
        val fieldFullSourceName = appendPath(sourcePath, fieldSourceName)

        val (renames, renameOnPath) = if ((fieldSourceName != field.name) || (predecessorChanged && includeIfPredecessorChanged)) {
          (renamesSoFar + (fieldFullName -> fieldFullSourceName), true)
        } else {
          (renamesSoFar, predecessorChanged)
        }

        field.dataType match {
          case st: StructType => getRenamesRecursively(fieldFullName, fieldFullSourceName, st, renames, renameOnPath)
          case at: ArrayType  => getStructInArray(at.elementType).fold(renames) { item =>
              getRenamesRecursively(fieldFullName, fieldFullSourceName, item, renames, renameOnPath)
            }
          case _              => renames
        }
      }
    }

    @tailrec
    def getStructInArray(dataType: DataType): Option[StructType] = {
      dataType match {
        case st: StructType => Option(st)
        case at: ArrayType => getStructInArray(at.elementType)
        case _ => None
      }
    }

    getRenamesRecursively("", "", schema, Map.empty, predecessorChanged = false)
  }

  /**
    * Get first array column's path out of complete path.
    *
    *  E.g if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" will be returned.
    *
    * @param path   The path to the attribute
    * @param schema The schema of the whole dataset
    * @return The path of the first array field or "" if none were found
    */
  def getFirstArrayPath(path: String, schema: StructType): String = {
    @tailrec
    def helper(remPath: Seq[String], pathAcc: Seq[String]): Seq[String] = {
      if (remPath.isEmpty) Seq() else {
        val currPath = (pathAcc :+ remPath.head).mkString(".")
        val currType = getFieldType(currPath, schema)
        currType match {
          case Some(_: ArrayType) => pathAcc :+ remPath.head
          case Some(_) => helper(remPath.tail, pathAcc :+ remPath.head)
          case None => Seq()
        }
      }
    }

    val pathToks = path.split('.')
    helper(pathToks, Seq()).mkString(".")
  }

  /**
    * Get paths for all array subfields of this given datatype
    */
  def getAllArraySubPaths(path: String, name: String, dt: DataType): Seq[String] = {
    val currPath = appendPath(path, name)
    dt match {
      case s: StructType => s.fields.flatMap(f => getAllArraySubPaths(currPath, f.name, f.dataType))
      case a@ArrayType(elType, nullable) => getAllArraySubPaths(path, name, elType) :+ currPath
      case _ => Seq()
    }
  }

  /**
    * Get all array columns' paths out of complete path.
    *
    *  E.g. if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" and "a.b.c.d" will be returned.
    *
    * @param path   The path to the attribute
    * @param schema The schema of the whole dataset
    * @return Seq of dot-separated paths for all array fields in the provided path
    */
  def getAllArraysInPath(path: String, schema: StructType): Seq[String] = {
    @tailrec
    def helper(remPath: Seq[String], pathAcc: Seq[String], arrayAcc: Seq[String]): Seq[String] = {
      if (remPath.isEmpty) arrayAcc else {
        val currPath = (pathAcc :+ remPath.head).mkString(".")
        val currType = getFieldType(currPath, schema)
        currType match {
          case Some(_: ArrayType) =>
            val strings = pathAcc :+ remPath.head
            helper(remPath.tail, strings, arrayAcc :+ strings.mkString("."))
          case Some(_) => helper(remPath.tail, pathAcc :+ remPath.head, arrayAcc)
          case None => arrayAcc
        }
      }
    }

    val pathToks = path.split("\\.")
    helper(pathToks, Seq(), Seq())
  }

  /**
    * For a given list of field paths determines the deepest common array path.
    *
    * For instance, if given 'a.b', 'a.b.c', 'a.b.c.d' where b and c are arrays the common deepest array
    * path is 'a.b.c'.
    *
    * If any of the arrays are on diverging paths this function returns None.
    *
    * The purpose of the function is to determine the order of explosions to be made before the dataframe can be
    * joined on a field inside an array.
    *
    * @param schema     A Spark schema
    * @param fieldPaths A list of paths to analyze
    * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
    */
  def getDeepestCommonArrayPath(schema: StructType, fieldPaths: Seq[String]): Option[String] = {
    val arrayPaths = fieldPaths.flatMap(path => getAllArraysInPath(path, schema)).distinct

    if (arrayPaths.nonEmpty && isCommonSubPath(arrayPaths: _*)) {
      Some(arrayPaths.maxBy(_.length))
    } else {
      None
    }
  }

  /**
    * For a field path determines the deepest array path.
    *
    * For instance, if given 'a.b.c.d' where b and c are arrays the deepest array is 'a.b.c'.
    *
    * @param schema    A Spark schema
    * @param fieldPath A path to analyze
    * @return Returns a common array path if there is one and None if any of the arrays are on diverging paths
    */
  def getDeepestArrayPath(schema: StructType, fieldPath: String): Option[String] = {
    val arrayPaths = getAllArraysInPath(fieldPath, schema)

    if (arrayPaths.nonEmpty) {
      Some(arrayPaths.maxBy(_.length))
    } else {
      None
    }
  }

  /**
    * For a given list of field paths determines if any path pair is a subset of one another.
    *
    * For instance,
    *  - 'a.b', 'a.b.c', 'a.b.c.d' have this property.
    *  - 'a.b', 'a.b.c', 'a.x.y' does NOT have it, since 'a.b.c' and 'a.x.y' have diverging subpaths.
    *
    * @param paths A list of paths to be analyzed
    * @return true if for all pathe the above property holds
    */
  def isCommonSubPath(paths: String*): Boolean = {
    def sliceRoot(paths: Seq[Seq[String]]): Seq[Seq[String]] = {
      paths.map(path => path.drop(1)).filter(_.nonEmpty)
    }

    var isParentCommon = true // For Seq() the property holds by [my] convention
    var restOfPaths: Seq[Seq[String]] = paths.map(_.split('.').toSeq).filter(_.nonEmpty)
    while (isParentCommon && restOfPaths.nonEmpty) {
      val parent = restOfPaths.head.head
      isParentCommon = restOfPaths.forall(path => path.head == parent)
      restOfPaths = sliceRoot(restOfPaths)
    }
    isParentCommon
  }

  /**
    * Get paths for all array fields in the schema
    *
    * @param schema The schema in which to look for array fields
    * @return Seq of dot separated paths of fields in the schema, which are of type Array
    */
  def getAllArrayPaths(schema: StructType): Seq[String] = {
    schema.fields.flatMap(f => getAllArraySubPaths("", f.name, f.dataType)).toSeq
  }

  /**
    * Append a new attribute to path or empty string.
    *
    * @param path      The dot-separated existing path
    * @param fieldName Name of the field to be appended to the path
    * @return The path with the new field appended or the field itself if path is empty
    */
  def appendPath(path: String, fieldName: String): String = {
    if (path.isEmpty) {
      fieldName
    } else if (fieldName.isEmpty) {
      path
    } else {
      s"$path.${fieldName}"
    }
  }

  /**
    * Determine if a datatype is a primitive one
    */
  def isPrimitive(dt: DataType): Boolean = dt match {
    case _: BinaryType | _: BooleanType | _: ByteType | _: DateType | _: DecimalType | _: DoubleType | _: FloatType | _: IntegerType | _: LongType | _: NullType | _: ShortType | _: StringType | _: TimestampType => true
    case _ => false
  }

  /**
    * Determine the name of a field
    * Will override to "sourcecolumn" in the Metadata if it exists
    *
    * @param field  field to work with
    * @return       Metadata "sourcecolumn" if it exists or field.name
    */
  def getFieldNameOverriddenByMetadata(field: StructField): String = {
    if (field.metadata.contains(MetadataKeys.SourceColumn)) {
      field.metadata.getString(MetadataKeys.SourceColumn)
    } else {
      field.name
    }
  }

  /**
    * For an array of arrays of arrays, ... get the final element type at the bottom of the array
    *
    * @param arrayType An array data type from a Spark dataframe schema
    * @return A non-array data type at the bottom of array nesting
    */
  @tailrec
  def getDeepestArrayType(arrayType: ArrayType): DataType = {
    arrayType.elementType match {
      case a: ArrayType => getDeepestArrayType(a)
      case b => b
    }
  }

  /**
    * Generate a unique column name
    *
    * @param prefix A prefix to use for the column name
    * @param schema An optional schema to validate if the column already exists (a very low probability)
    * @return A name that can be used as a unique column name
    */
  def getUniqueName(prefix: String, schema: Option[StructType]): String = {
    schema match {
      case None =>
        s"${prefix}_${Random.nextLong().abs}"
      case Some(sch) =>
        var exists = true
        var columnName = ""
        while (exists) {
          columnName = s"${prefix}_${Random.nextLong().abs}"
          exists = sch.fields.exists(_.name.compareToIgnoreCase(columnName) == 0)
        }
        columnName
    }
  }

  /**
    * Get a closest unique column name
    *
    * @param desiredName A prefix to use for the column name
    * @param schema      A schema to validate if the column already exists
    * @return A name that can be used as a unique column name
    */
  def getClosestUniqueName(desiredName: String, schema: StructType): String = {
    var exists = true
    var columnName = ""
    var i = 0
    while (exists) {
      columnName = if (i == 0) desiredName else s"${desiredName}_$i"
      exists = schema.fields.exists(_.name.compareToIgnoreCase(columnName) == 0)
      i += 1
    }
    columnName
  }

  /**
    * Checks if a casting between types always succeeds
    *
    * @param sourceType A type to be casted
    * @param targetType A type to be casted to
    * @return true if casting never fails
    */
  def isCastAlwaysSucceeds(sourceType: DataType, targetType: DataType): Boolean = {
    (sourceType, targetType) match {
      case (_: StructType, _) | (_: ArrayType, _) => false
      case (a, b) if a == b => true
      case (_, _: StringType) => true
      case (_: ByteType, _: ShortType | _: IntegerType | _: LongType) => true
      case (_: ShortType, _: IntegerType | _: LongType) => true
      case (_: IntegerType, _: LongType) => true
      case (_: DateType, _: TimestampType) => true
      case _ => false
    }
  }

  /**
    * Checks if a field is an array
    *
    * @param schema        A schema
    * @param fieldPathName A field to check
    * @return true if the specified field is an array
    */
  def isArray(schema: StructType, fieldPathName: String): Boolean = {
    @tailrec
    def arrayHelper(arrayField: ArrayType, path: Seq[String]): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0

      arrayField.elementType match {
        case st: StructType => structHelper(st, path.tail)
        case ar: ArrayType => arrayHelper(ar, path)
        case _ =>
          if (!isLeaf) {
            throw new IllegalArgumentException(
              s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
          }
          false
      }
    }

    def structHelper(structField: StructType, path: Seq[String]): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isArray = false
      structField.fields.foreach(field =>
        if (field.name == currentField) {
          field.dataType match {
            case st: StructType =>
              if (!isLeaf) {
                isArray = structHelper(st, path.tail)
              }
            case ar: ArrayType =>
              if (isLeaf) {
                isArray = true
              } else {
                isArray = arrayHelper(ar, path)
              }
            case _ =>
              if (!isLeaf) {
                throw new IllegalArgumentException(
                  s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
              }
          }
        }
      )
      isArray
    }

    val path = fieldPathName.split('.')
    structHelper(schema, path)
  }

  /**
    * Checks if a field is an array that is not nested in another array
    *
    * @param schema        A schema
    * @param fieldPathName A field to check
    * @return true if a field is an array that is not nested in another array
    */
  def isNonNestedArray(schema: StructType, fieldPathName: String): Boolean = {
    def structHelper(structField: StructType, path: Seq[String]): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isArray = false
      structField.fields.foreach(field =>
        if (field.name == currentField) {
          field.dataType match {
            case st: StructType =>
              if (!isLeaf) {
                isArray = structHelper(st, path.tail)
              }
            case ar: ArrayType =>
              if (isLeaf) {
                isArray = true
              }
            case _ =>
              if (!isLeaf) {
                throw new IllegalArgumentException(
                  s"Primitive fields cannot have child fields $currentField is a primitive in $fieldPathName")
              }
          }
        }
      )
      isArray
    }

    val path = fieldPathName.split('.')
    structHelper(schema, path)
  }

  /**
    * Checks if a field is the only field in a struct
    *
    * @param schema A schema
    * @param column A column to check
    * @return true if the column is the only column in a struct
    */
  def isOnlyField(schema: StructType, column: String): Boolean = {
    def structHelper(structField: StructType, path: Seq[String]): Boolean = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isOnlyField = false
      structField.fields.foreach(field =>
        if (field.name == currentField) {
          if (isLeaf) {
            isOnlyField = structField.fields.length == 1
          } else {
            field.dataType match {
              case st: StructType =>
                isOnlyField = structHelper(st, path.tail)
              case _: ArrayType =>
                throw new IllegalArgumentException(
                  s"SchemaUtils.isOnlyField() does not support checking struct fields inside an array")
              case _ =>
                throw new IllegalArgumentException(
                  s"Primitive fields cannot have child fields $currentField is a primitive in $column")
            }
          }
        }
      )
      isOnlyField
    }
    val path = column.split('.')
    structHelper(schema, path)
  }

  /**
    * Converts a fully qualified field name (including its path, e.g. containing fields) to a unique field name without
    * dot notation
    * @param path  the fully qualified field name
    * @return      unique top level field name
    */
  def unpath(path: String): String = {
    path.replace("_", "__")
        .replace('.', '_')
  }

}
