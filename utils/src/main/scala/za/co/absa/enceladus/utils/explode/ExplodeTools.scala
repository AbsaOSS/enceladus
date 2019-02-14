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

package za.co.absa.enceladus.utils.explode

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils._

object ExplodeTools {
  // scalastyle:off method.length
  // scalastyle:off null

  private val deconstructedColumnName = "electron"
  private val explosionTmpColumnName = "proton"
  private val nullRestoredTmpColumnName = "neutron"
  private val transientColumnName = "higgs_boson"

  /**
    * Explodes a specific array inside a dataframe in context. Returns a new dataframe and a new context.
    * Context can be used to revert all explosions back.
    *
    * @param arrayFieldName   An array field name to be exploded. It can be inside a nested struct, but cannot be nested
    *                         inside another array. If that is the case you need to explode the topmost array first.
    * @param inputDf          A DataFrame that contains an array
    * @param explosionContext A context returned by previous explosions. If you do several explosions on the top of
    *                         each other it is very important to pass the previous context here so all explosions could
    *                         be reverted
    * @return A pair containing an exploded DataFrame and an explosion context.
    */
  def explodeArray(arrayFieldName: String,
                   inputDf: DataFrame,
                   explosionContext: ExplosionContext = ExplosionContext()): (DataFrame, ExplosionContext) = {

    validateArrayField(inputDf.schema, arrayFieldName)

    val explodedColumnName = getUniqueName(explosionTmpColumnName, Some(inputDf.schema))
    val explodedIdName = getRootLevelPrefix(arrayFieldName, "id", inputDf.schema)
    val explodedIndexName = getRootLevelPrefix(arrayFieldName, "idx", inputDf.schema)
    val explodedSizeName = getRootLevelPrefix(arrayFieldName, "size", inputDf.schema)

    // Adding an unique row id so we can reconstruct the array later by grouping by that id
    val dfWithId = inputDf.withColumn(explodedIdName, monotonically_increasing_id())

    // Exploding...
    // The '-1' value as an array size indicates that the array field is null. This is to distinguish
    // between array field being empty or null
    val nullArrayIndicator = -1
    val explodedDf = dfWithId
      .select(dfWithId.schema.map(a => col(a.name)) :+
        when(col(arrayFieldName).isNull,
          nullArrayIndicator).otherwise(size(col(arrayFieldName))).as(explodedSizeName) :+
        posexplode_outer(col(arrayFieldName)).as(Seq(explodedIndexName, explodedColumnName)): _*)

    val explodedColRenamed = nestedRenameReplace(explodedDf, explodedColumnName, arrayFieldName)

    val newExplosion = Explosion(arrayFieldName, explodedIdName, explodedIndexName, explodedSizeName)
    val newContext = explosionContext.copy(explosions = newExplosion +: explosionContext.explosions)
    (explodedColRenamed, newContext)
  }

  /**
    * Reverts all explosions done by explodeArray().
    * An explosion context should be a context returned by the latest explosion.
    *
    * @param inputDf          A DataFrame that contains an exploded array
    * @param explosionContext A context returned by explodeArray()
    * @param errorColumn      An optional error column to combine during implosion. It should be a top level array.
    * @return A dataframe containing restored ('imploded') arrays.
    */
  def revertAllExplosions(inputDf: DataFrame,
                          explosionContext: ExplosionContext,
                          errorColumn: Option[String] = None): DataFrame = {
    explosionContext.explosions.foldLeft(inputDf)((df, explosion) => {
      revertSingleExplosion(df, explosion, errorColumn)
    })
  }

  /**
    * Reverts aa particular explode made by explodeArray().
    * If there were several explodes they should be reverted in FILO order
    *
    * @param inputDf     A DataFrame that contains an exploded array
    * @param explosion   An explosion object containing all data necessary to revert the explosion
    * @param errorColumn An optional error column to combine during implosion. It should be a top level array.
    * @return A dataframe containing restored ('imploded') arrays.
    */
  def revertSingleExplosion(inputDf: DataFrame,
                            explosion: Explosion,
                            errorColumn: Option[String] = None): DataFrame = {

    errorColumn.foreach(validateErrorColumnField(inputDf.schema, _))

    val isNested = explosion.arrayFieldName.contains('.')

    val (decDf, decField) = if (isNested) {
      deconstructNestedColumn(inputDf, explosion.arrayFieldName)
    } else {
      (inputDf, explosion.arrayFieldName)
    }

    val orderByCol = col(explosion.indexFieldName)
    val groupedCol = col(explosion.idFieldName)

    // Do not group by columns that are explosion artifacts
    val allOtherColumns = inputDf.schema
      .filter(a => a.name != explosion.idFieldName
        && a.name != explosion.indexFieldName
        && (a.name != explosion.arrayFieldName || isNested)
        && (errorColumn.isEmpty || a.name != errorColumn.get)
      )
      .map(a => col(a.name))

    // Implode as a temporary column
    val tmpColName = getUniqueName(explosionTmpColumnName, Some(inputDf.schema))

    // Implode
    val dfImploded = errorColumn match {
      case None =>
        decDf
          .orderBy(orderByCol).groupBy(groupedCol +: allOtherColumns: _*)
          .agg(collect_list(decField).as(tmpColName))
      case Some(errorCol) =>
        // Implode taking into account the error column
        // Errors should be collected, flattened and made distinct
        decDf
          .orderBy(orderByCol).groupBy(groupedCol +: allOtherColumns: _*)
          .agg(collect_list(decField).as(tmpColName),
            array_distinct(flatten(collect_list(col(errorCol)))).as(errorCol))
    }

    // Restore null values to yet another temporary field
    val tmpColName2 = getUniqueName(nullRestoredTmpColumnName, Some(inputDf.schema))
    val nullsRestored = dfImploded
      .withColumn(tmpColName2, when(col(explosion.sizeFieldName) > 0, col(tmpColName))
        otherwise when(col(explosion.sizeFieldName) === 0, typedLit(Array())).otherwise(null)
      )

    val dfArraysRestored = nestedRenameReplace(nullsRestored, tmpColName2, explosion.arrayFieldName)

    dfArraysRestored
      // Drop the temporary column
      .drop(col(tmpColName))
      // Drop the array size column
      .drop(col(explosion.sizeFieldName))
      // restore original record order
      .orderBy(groupedCol)
      // remove monotonic id created during explode
      .drop(groupedCol)
  }

  /**
    * Takes a field name nested in a struct and moves it out to the root level as a top level column
    *
    * @param inputDf    A dataframe to process
    * @param columnName A nested column to process
    * @return A transformed dataframe
    **/
  def deconstructNestedColumn(inputDf: DataFrame, columnName: String): (DataFrame, String) = {
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      val newFields = schema.fields.flatMap(field => {
        if (field.name != currentField) {
          Seq(getFullFieldPath(parentCol, field.name).as(field.name))
        } else {
          if (isLeaf) {
            // Removing the field from the struct
            if (schema.size == 1) {
              // If removing the last element of a struct makes it a struct with no fields empty.
              // This is not allowed in Spark. We need to compensate the struct with a transient column.
              // The transient column will be replaces by imploded column after implosion.
              Seq(lit(0).as(transientColumnName))
            } else {
              Nil
            }
          } else {
            field.dataType match {
              case st: StructType =>
                Seq(struct(processStruct(st, path.tail, Some(getFullFieldPath(parentCol, field.name))): _*)
                  .as(field.name))
              case _ =>
                throw new IllegalArgumentException(s"$currentField is not a struct in $columnName")
            }
          }
        }
      })
      newFields
    }

    val newFieldName = getClosestUniqueName(deconstructedColumnName, inputDf.schema)
    val resultDf = inputDf.select(processStruct(inputDf.schema, columnName.split('.'), None)
      :+ col(columnName).as(newFieldName): _*)
    (resultDf, newFieldName)
  }

  /**
    * Renames a column `columnFrom` to `columnTo` replacing the original column and putting the resulting column
    * under the same struct level of nesting as `columnFrom`.
    *
    * @param inputDf    A dataframe to process
    * @param columnFrom A column name that needs to be put into a nested struct
    * @param columnTo   A column name that `columnFrom` should have after it is renamed
    * @return A transformed dataframe
    **/
  def nestedRenameReplace(inputDf: DataFrame, columnFrom: String, columnTo: String): DataFrame = {
    if (!columnTo.contains('.') && !columnFrom.contains('.')) {
      var isColumnToFound = false
      val newFields = inputDf.schema.fields.flatMap(field =>
        if (field.name == columnTo) {
          isColumnToFound = true
          Seq(col(columnFrom).as(columnTo))
        } else if (field.name == columnFrom) {
          Nil
        } else {
          Seq(col(field.name))
        }
      )
      val newFields2 = if (isColumnToFound) newFields else newFields :+ col(columnFrom).as(columnTo)
      inputDf.select(newFields2: _*)
    } else {
      putFieldIntoNestedStruct(inputDf, columnFrom, columnTo.split('.'))
    }
  }

  private def getFullFieldPath(parentCol: Option[Column], fieldName: String): Column = {
    parentCol match {
      case None => col(fieldName)
      case Some(parent) => parent.getField(fieldName)
    }
  }

  private def getRootLevelPrefix(fieldName: String, prefix: String, schema: StructType): String = {
    getClosestUniqueName(s"${fieldName}_$prefix", schema)
      .replaceAll("\\.", "_")
  }

  private def extructFieldFromStruct(df: DataFrame, structFieldName: String): (DataFrame, Column) = {
    val tmpColName = getUniqueName("tmp_ext", Some(df.schema))
    (df.withColumn(tmpColName, col(structFieldName)), col(tmpColName))
  }

  private def putFieldIntoNestedStruct(df: DataFrame, columnFrom: String, pathTo: Seq[String]): DataFrame = {
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isFound = false

      val newFields = schema.fields.flatMap(field => {
        if (field.name == columnFrom || field.name == transientColumnName) {
          // This removes the original column name (if any) and the transient column
          Nil
        } else if (field.name == currentField) {
          field.dataType match {
            case _ if path.lengthCompare(1) == 0 =>
              isFound = true
              Seq(col(columnFrom).as(currentField))
            case st: StructType =>
              val newFields = processStruct(st, path.tail, Some(getFullFieldPath(parentCol, field.name)))
              if (newFields.lengthCompare(1) == 0) {
                // a struct that can be null
                val fld = newFields.head
                Seq(when(fld.isNotNull, struct(newFields: _*)).otherwise(null).as(field.name))
              } else {
                // Normat struct
                Seq(struct(newFields: _*).as(field.name))
              }
            case _ =>
              throw new IllegalArgumentException(s"$currentField is not a struct in ${pathTo.mkString(".")}")
          }
        } else {
          Seq(getFullFieldPath(parentCol, field.name).as(field.name))
        }
      })
      if (!isFound && isLeaf) {
        val c = col(columnFrom)
        newFields :+ when(c.isNotNull, c).otherwise(null).as(currentField)
      } else {
        newFields
      }
    }

    df.select(processStruct(df.schema, pathTo, None): _*)
  }

  private def validateArrayField(schema: StructType, fieldName: String): Unit = {
    if (!SchemaUtils.isArray(schema, fieldName)) {
      throw new IllegalArgumentException(s"$fieldName is not an array.")
    }

    if (!SchemaUtils.isNonNestedArray(schema, fieldName)) {
      throw new IllegalArgumentException(
        s"$fieldName is an array that is nested in other arrays. Need to explode top level array first.")
    }
  }

  private def validateErrorColumnField(schema: StructType, fieldName: String): Unit = {
    if (fieldName.contains('.')) {
      throw new IllegalArgumentException(s"An error column $fieldName cannot be nested.")
    }
    if (!SchemaUtils.isArray(schema, fieldName)) {
      throw new IllegalArgumentException(s"An error column $fieldName is not an array.")
    }
  }
}
