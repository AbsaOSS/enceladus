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
import za.co.absa.enceladus.utils.schema.SchemaUtils._

object ExplodeTools {
  // scalastyle:off method.length
  // scalastyle:off null

  /**
    * Explodes a specific array inside a dataframe in context. Returns a new dataframe and a new context.
    * Context can be used to revert all explosions back
    */
  def explodeArray(arrayFieldName: String,
                   df: DataFrame,
                   explosionContext: ExplodeContext = ExplodeContext()): (DataFrame, ExplodeContext) = {

    // TODO: Handle a case when the input fields is not an array

    // TODO: Handle the case when the input is field is an array inside an array

    val explodedColumnName = getUniqueName("tmp", Some(df.schema))
    val explodedIdName = getRootLevelPrefix(arrayFieldName, "id", df.schema)
    val explodedIndexName = getRootLevelPrefix(arrayFieldName, "idx", df.schema)
    val explodedSizeName = getRootLevelPrefix(arrayFieldName, "size", df.schema)

    // Adding an unique row id so we can reconstruct the array later by grouping by that id
    val dfWithId = df.withColumn(explodedIdName, monotonically_increasing_id())

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
    * Reverts all explosions done by explode array().
    * Context can be used to revert all explosions back
    */
  def revertAllExplosions(inputDf: DataFrame, explosionContext: ExplodeContext): DataFrame = {
    explosionContext.explosions.foldLeft(inputDf)((df, explosion) => {
      revertSingleExplosion(df, explosion)
    })
  }

  /**
    * Reverts aa particular explode made by explodeArray().
    * If there were several explodes they should be reverted in FILO order
    */
  def revertSingleExplosion(df: DataFrame, explosion: Explosion): DataFrame = {
    if (explosion.arrayFieldName.contains('.')) {
      revertNestedFieldExplosion(df, explosion)
    } else {
      revertTopLevelFieldExplosion(df, explosion)
    }
  }

  private def revertTopLevelFieldExplosion(df: DataFrame, explosion: Explosion): DataFrame = {
    val orderByCol = col(explosion.indexFieldName)
    val groupedCol = col(explosion.idFieldName)
    val structCol = col(explosion.arrayFieldName)
    val rootOfArrayField = explosion.arrayFieldName.split('.').head

    // Do not group by columns that are explosion artifacts
    val allOtherColumns = df.schema
      .filter(a => a.name != explosion.idFieldName
        && a.name != explosion.indexFieldName
        && a.name != rootOfArrayField
      )
      .map(a => col(a.name))

    // Implode as a temporaty field
    val tmpColName = getUniqueName("tmp", Some(df.schema))

    // Implode
    val dfImploded = df
      .orderBy(orderByCol).groupBy(groupedCol +: allOtherColumns: _*)
      .agg(collect_list(structCol). as(tmpColName))

    // Restore null values to yet another temporary field
    val tmpColName2 = getUniqueName("tmp2", Some(df.schema))
    val nullsRestored = dfImploded
      .withColumn(tmpColName2, when(col(explosion.sizeFieldName) >= 0, col(tmpColName)).otherwise(null))

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

  def revertNestedFieldExplosion(df: DataFrame, explosion: Explosion): DataFrame = {
    val (decDf, decField) = deconstruct(df, explosion.arrayFieldName)

    val orderByCol = col(explosion.indexFieldName)
    val groupedCol = col(explosion.idFieldName)

    // Do not group by columns that are explosion artifacts
    val allOtherColumns = df.schema
      .filter(a => a.name != explosion.idFieldName
        && a.name != explosion.indexFieldName
      )
      .map(a => col(a.name))

    // Implode as a temporaty field
    val tmpColName = getUniqueName("tmp", Some(df.schema))

    // Implode
    val dfImploded = decDf
      .orderBy(orderByCol).groupBy(groupedCol +: allOtherColumns: _*)
      .agg(collect_list(decField). as(tmpColName))

    // Restore null values to yet another temporary field
    val tmpColName2 = getUniqueName("tmp2", Some(df.schema))
    val nullsRestored = dfImploded
      .withColumn(tmpColName2, when(col(explosion.sizeFieldName) >= 0, col(tmpColName)).otherwise(null))

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

  private def getRootLevelPrefix(fieldName: String, prefix: String, schema: StructType): String = {
    getClosestUniqueName(s"${fieldName}_$prefix", schema)
      .replaceAll("\\.", "_")
  }

  def extructFieldFromStruct(df: DataFrame, structFieldName: String): (DataFrame, Column) = {
    val tmpColName = getUniqueName("tmp_ext", Some(df.schema))
    (df.withColumn(tmpColName, col(structFieldName)), col(tmpColName))
  }

  /**
    * Renames a column `columnFrom` to `columnTo` replacing the original column and putting the resulting column
    * under the same struct level of nesting as `columnFrom`
    *
    **/
  def nestedRenameReplace(df: DataFrame, columnFrom: String, columnTo: String): DataFrame = {
    if (!columnTo.contains('.') && !columnFrom.contains('.')) {
      var isColumnToFound = false
      val newFields = df.schema.fields.flatMap(field =>
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
      df.select(newFields2: _*)
    } else {
      putFieldIntoNestedStruct(df, columnFrom, columnTo.split('.'))
    }
  }

  private def getFullFieldPath(parentCol: Option[Column], fieldName: String): Column = {
    parentCol match {
      case None => col(fieldName)
      case Some(parent) => parent.getField(fieldName)
    }
  }

  private def putFieldIntoNestedStruct(df: DataFrame, columnFrom: String, pathTo: Seq[String]): DataFrame = {
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isFound = false

      val newFields = schema.fields.flatMap(field => {
        if (field.name == columnFrom) {
          Nil
        } else if (field.name == currentField) {
          field.dataType match {
            case _ if path.lengthCompare(1) == 0 =>
              isFound = true
              Seq(col(columnFrom).as(currentField))
            case st: StructType =>
              Seq(struct(processStruct(st, path.tail, Some(getFullFieldPath(parentCol, field.name))): _*)
                .as(field.name))
            case _ =>
              throw new IllegalArgumentException(s"$currentField is not a struct in ${pathTo.mkString(".")}")
          }
        } else {
          Seq(getFullFieldPath(parentCol, field.name).as(field.name))
        }
      })
      if (!isFound && isLeaf) {
        newFields :+ col(columnFrom).as(currentField)
      } else {
        newFields
      }
    }

    df.select(processStruct(df.schema, pathTo, None): _*)
  }

  /** Takes a field name nested in a struct and moves it to the root level as a setmprry field */
  def deconstruct(df: DataFrame, fieldName: String): (DataFrame, String) = {
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      val newFields = schema.fields.flatMap(field => {
        if (field.name != currentField) {
          Seq(getFullFieldPath(parentCol, field.name).as(field.name))
        } else {
          if (isLeaf) {
            // Removing the field from the struct
            Nil
          } else {
            field.dataType match {
              case st: StructType =>
                Seq(struct(processStruct(st, path.tail, Some(getFullFieldPath(parentCol, field.name))): _*)
                  .as(field.name))
              case _ =>
                throw new IllegalArgumentException(s"$currentField is not a struct in $fieldName")
            }
          }
        }
      })
      newFields
    }

    val newFieldName = getClosestUniqueName(s"proton", df.schema)
    val resultDf = df.select(processStruct(df.schema, fieldName.split('.'), None)
      :+ col(fieldName).as(newFieldName): _*)
    (resultDf, newFieldName)
  }

}
