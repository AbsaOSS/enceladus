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

package za.co.absa.enceladus.utils.schema

import org.apache.spark.sql.types._
import scala.util.Try

object SchemaUtils {

  /**
   * Get a type of a field from a text path and a given schema
   *
   *  @param path The dot-separated path to the field
   *  @param schema The schema which should contain the specified path
   *  @return Some(the type of the field) or None if the field does not exist
   */
  def getFieldType(path: String, schema: StructType): Option[DataType] = {
    def typeHelper(pt: List[String], st: DataType): Option[DataType] = {
      if (pt.isEmpty) {
        Some(st)
      } else {
        st match {
          case str: StructType              => Try { typeHelper(pt.tail, str(pt.head).dataType) }.getOrElse(None)
          case ArrayType(el: StructType, _) => Try { typeHelper(pt.tail, el(pt.head).dataType) }.getOrElse(None)
          case _                            => None
        }
      }
    }

    val pathTokens = path.split("\\.").toList
    typeHelper(pathTokens, schema)
  }

   /**
   * Get nullability of a field from a text path and a given schema
   *
   *  @param path The dot-separated path to the field
   *  @param schema The schema which should contain the specified path
   *  @return Some(nullable) or None if the field does not exist
   */
  def getFieldNullability(path: String, schema: StructType): Option[Boolean] = {
    def typeHelper(pt: List[String], st: DataType, nl: Option[Boolean]): Option[Boolean] = {
      if (pt.isEmpty) nl
      else {
        st match {
          case str: StructType              => Try { typeHelper(pt.tail, str(pt.head).dataType, Some(str.apply(pt.head).nullable)) }.getOrElse(None)
          case ArrayType(el: StructType, _) => Try { typeHelper(pt.tail, el(pt.head).dataType, Some(el(pt.head).nullable)) }.getOrElse(None)
          case _                            => None
        }
      }
    }

    val pathTokens = path.split("\\.").toList
    typeHelper(pathTokens, schema, None)
  }


  /**
   * Get first array column's path out of complete path.
   *
   *  E.g if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" will be returned.
   *
   *  @param path The path to the attribute
   *  @param schema The schema of the whole dataset
   *  @return The path of the first array field or "" if none were found
   */
  def getFirstArrayPath(path: String, schema: StructType) = {
    def helper(remPath: Seq[String], pathAcc: Seq[String]): Seq[String] = {
      if (remPath.isEmpty) Seq() else {
        val currPath = (pathAcc :+ remPath.head).mkString(".")
        val currType = getFieldType(currPath, schema)
        currType match {
          case Some(_: ArrayType) => pathAcc :+ remPath.head
          case Some(_)            => helper(remPath.tail, pathAcc :+ remPath.head)
          case None               => Seq()
        }
      }
    }

    val pathToks = path.split("\\.")
    helper(pathToks, Seq()).mkString(".")
  }

  /**
   * Get paths for all array subfields of this given datatype
   */
  def getAllArraySubPaths(path: String, name: String, dt: DataType): Seq[String] = {
    val currPath = (if (path.isEmpty) name else if (name.isEmpty()) path else s"$path.${name}")
    dt match {
      case s: StructType                   => s.fields.flatMap(f => getAllArraySubPaths(currPath, f.name, f.dataType))
      case a @ ArrayType(elType, nullable) => getAllArraySubPaths(path, name, elType) :+ currPath
      case _                               => Seq()
    }
  }

   /**
   *  Get all array columns' paths out of complete path.
   *
   *  E.g. if the path argument is "a.b.c.d.e" where b and d are arrays, "a.b" and "a.b.c.d" will be returned.
   *
   *  @param path The path to the attribute
   *  @param schema The schema of the whole dataset
   *  @return Seq of dot-separated paths for all array fields in the provided path
   */
  def getAllArraysInPath(path: String, schema: StructType): Seq[String] = {
    def helper(remPath: Seq[String], pathAcc: Seq[String], arrayAcc: Seq[String]): Seq[String] = {
      if (remPath.isEmpty) arrayAcc else {
        val currPath = (pathAcc :+ remPath.head).mkString(".")
        val currType = getFieldType(currPath, schema)
        currType match {
          case Some(_: ArrayType) =>
            val strings = pathAcc :+ remPath.head
            helper(remPath.tail, strings, arrayAcc :+ strings.mkString("."))
          case Some(_)            => helper(remPath.tail, pathAcc :+ remPath.head, arrayAcc)
          case None               => arrayAcc
        }
      }
    }

    val pathToks = path.split("\\.")
    helper(pathToks, Seq(), Seq())
  }

  /**
   * Get paths for all array fields in the schema
   *
   * @param schema The schema in which to look for array fields
   * @return Seq of dot separated paths of fields in the schema, which are of type Array
   */
  def getAllArrayPaths(schema: StructType) : Seq[String] = {
    schema.fields.flatMap(f => getAllArraySubPaths("", f.name, f.dataType)).toSeq
  }

  /**
   * Append a new attribute to path or empty string.
   *
   * @param path The dot-separated existing path
   * @param fieldName Name of the field to be appended to the path
   * @return The path with the new field appended or the field itself if path is empty
   */
  private[enceladus] def appendPath(path: String, fieldName: String) = {
    if (path.isEmpty()) fieldName else s"$path.$fieldName"
  }

  /**
   * Determine if a datatype is a primitive one
   */
  def isPrimitive(dt: DataType) = dt match {
    case _: BinaryType | _: BooleanType | _: ByteType | _: DateType | _: DecimalType | _: DoubleType | _: FloatType | _: IntegerType | _: LongType | _: NullType | _: ShortType | _: StringType | _: TimestampType => true
    case _ => false
  }

  /**
    * Determine the name of a field
    * Will override to "sourcecolumn" in the Metadata if it exists
    * @param field
    * @return Metadata "sourcecolumn" if it exists or field.name
    */
  def getFieldNameOverriddenByMetadata(field: StructField): String = {
    if (field.metadata.contains("sourcecolumn")) {
      field.metadata.getString("sourcecolumn")
    }
    else
      field.name
  }


}
