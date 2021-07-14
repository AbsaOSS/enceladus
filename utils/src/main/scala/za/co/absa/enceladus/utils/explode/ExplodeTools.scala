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

package za.co.absa.enceladus.utils.explode

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits._
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils._
import za.co.absa.spark.hats.Extensions._

object ExplodeTools {
  // scalastyle:off null

  private val log = LogManager.getLogger(this.getClass)

  case class DeconstructedNestedField (df: DataFrame, deconstructedField: String, transientField: Option[String])

  /**
    * Explodes all arrays within the path.
    * Context can be used to revert all explosions back.
    *
    * @param columnPathName   An column to be exploded. It can be nested inside array or several levels of array nesting
    * @param inputDf          A DataFrame that contains an array
    * @param explosionContext A context returned by previous explosions. If you do several explosions on the top of
    *                         each other it is very important to pass the previous context here so all explosions could
    *                         be reverted
    * @return A pair containing an exploded DataFrame and an explosion context.
    */
  def explodeAllArraysInPath(columnPathName: String,
                             inputDf: DataFrame,
                             explosionContext: ExplosionContext = ExplosionContext()): (DataFrame, ExplosionContext) = {
    val arrays = SchemaUtils.getAllArraysInPath(columnPathName, inputDf.schema)
    arrays.foldLeft(inputDf, explosionContext)(
      (contextPair, arrayColName) => {
        contextPair match {
          case (df, context) =>
            log.info(s"Exploding $arrayColName...")
            explodeArray(arrayColName, df, context)
        }
      })
  }

  /**
    * Explodes a specific array inside a dataframe in context. Returns a new dataframe and a new context.
    * Context can be used to revert all explosions back.
    *
    * @param arrayColPathName An array field name to be exploded. It can be inside a nested struct, but cannot be nested
    *                         inside another array. If that is the case you need to explode the topmost array first.
    * @param inputDf          A DataFrame that contains an array
    * @param explosionContext A context returned by previous explosions. If you do several explosions on the top of
    *                         each other it is very important to pass the previous context here so all explosions could
    *                         be reverted
    * @return A pair containing an exploded DataFrame and an explosion context.
    */
  def explodeArray(arrayColPathName: String,
                   inputDf: DataFrame,
                   explosionContext: ExplosionContext = ExplosionContext()): (DataFrame, ExplosionContext) = {

    validateArrayField(inputDf.schema, arrayColPathName)

    val explodedColumnName = getUniqueName(explosionTmpColumnName, Some(inputDf.schema))
    val explodedIdName = getRootLevelPrefix(arrayColPathName, "id", inputDf.schema)
    val explodedIndexName = getRootLevelPrefix(arrayColPathName, "idx", inputDf.schema)
    val explodedSizeName = getRootLevelPrefix(arrayColPathName, "size", inputDf.schema)

    // Adding an unique row id so we can reconstruct the array later by grouping by that id
    val dfWithId = inputDf.withColumn(explodedIdName, monotonically_increasing_id())

    // Add a transient field if we are exploding an array that is an only column of a struct.
    // The rationale for this is that otherwise a struct with all null fields will be treated as null
    // And after reverting the explosion empty structs will become nulls.
    // Spark works fine if the array is not the only field in the struct. So we add a transient field
    // that will exist only between explosion and its restoration.
    val (dfWithTransientField, superTransientFieldName) = if (isOnlyField(inputDf.schema, arrayColPathName)) {
      val (newDf, transientFldName) = addSuperTransientField(dfWithId, arrayColPathName)
      (newDf, Some(transientFldName))
    } else {
      (dfWithId, None)
    }

    // Exploding...
    // The '-1' value as an array size indicates that the array field is null. This is to distinguish
    // between array field being empty or null
    val nullArrayIndicator = -1
    val explodedDf = dfWithTransientField
      .select(dfWithId.schema.map(a => col(a.name)) :+
        when(col(arrayColPathName).isNull,
          nullArrayIndicator).otherwise(size(col(arrayColPathName))).as(explodedSizeName) :+
        posexplode_outer(col(arrayColPathName)).as(Seq(explodedIndexName, explodedColumnName)): _*)

    val explodedColRenamed = nestedRenameReplace(explodedDf, explodedColumnName, arrayColPathName)

    val newExplosion = Explosion(arrayColPathName, explodedIdName, explodedIndexName, explodedSizeName,
      superTransientFieldName)
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
  // scalastyle:off method.length
  def revertSingleExplosion(inputDf: DataFrame,
                            explosion: Explosion,
                            errorColumn: Option[String] = None): DataFrame = {
    log.info(s"Reverting explosion $explosion...")

    errorColumn.foreach(validateErrorColumnField(inputDf.schema, _))

    val isNested = explosion.arrayFieldName.contains('.')

    val (decDf, deconstructedField, transientColumn) = if (isNested) {
      val deconstructedData = deconstructNestedColumn(inputDf, explosion.arrayFieldName)
      DeconstructedNestedField.unapply(deconstructedData).get
    } else {
      (inputDf, explosion.arrayFieldName, None)
    }

    val orderByInsideArray = col(explosion.indexFieldName)
    val orderByRecordCol = col(explosion.idFieldName)

    // Do not group by columns that are explosion artifacts
    val groupByColumns = inputDf.schema
      .filter(a => a.name != explosion.indexFieldName
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
          .orderBy(orderByRecordCol, orderByInsideArray)
          .groupBy(groupByColumns: _*)
          .agg(collect_list(deconstructedField).as(tmpColName))
      case Some(errorCol) =>
        // Implode taking into account the error column
        // Errors should be collected, flattened and made distinct
        // decDf.schema: "errCol: array (nullable = true)", while the result would contain "errCol: array (nullable = false)"
        // bc. collect_list. See issue #1818
        decDf.orderBy(orderByRecordCol, orderByInsideArray)
          .groupBy(groupByColumns: _*)
          .agg(collect_list(deconstructedField).as(tmpColName),
            array_distinct(flatten(collect_list(col(errorCol)))).as(errorCol))
          .withNullableColumnState(errorCol, true) // explicitly nullable: expected schema outcome, see issue #1818
    }

    // Restore null values to yet another temporary field
    val tmpColName2 = getUniqueName(nullRestoredTmpColumnName, Some(inputDf.schema))
    val nullsRestored = dfImploded
      .withColumn(tmpColName2, when(col(explosion.sizeFieldName) > 0, col(tmpColName))
        .otherwise(when(col(explosion.sizeFieldName) === 0, typedLit(Array())).otherwise(null))
      )

    val dfArraysRestored = nestedRenameReplace(nullsRestored, tmpColName2, explosion.arrayFieldName,
      transientColumn)

    val dfTransientRestored = explosion.superTransientFieldName match {
      case Some(transientField) => dfArraysRestored.nestedDropColumn(transientField)
      case None => dfArraysRestored
    }

    dfTransientRestored
      // Drop the temporary column
      .drop(col(tmpColName))
      // Drop the array size column
      .drop(col(explosion.sizeFieldName))
      // restore original record order
      .orderBy(orderByRecordCol)
      // remove monotonic id created during explode
      .drop(orderByRecordCol)
  }
  // scalastyle:on method.length

  /**
    * Takes a field name nested in a struct and moves it out to the root level as a top level column
    *
    * @param inputDf    A dataframe to process
    * @param columnName A nested column to process
    * @return A transformed dataframe
    **/
  def deconstructNestedColumn(inputDf: DataFrame, columnName: String): DeconstructedNestedField = {
    var transientColName: Option[String] = None
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      val newFields = schema.fields.flatMap(field => {
        if (field.name != currentField) {
          Seq(getFullFieldPath(parentCol, field.name).as(field.name))
        } else {
          if (isLeaf) {
            // Removing the field from the struct replacing it with a transient field
            val name = getClosestUniqueName(transientColumnName, schema)
            transientColName = Some(name)
             Seq(lit(0).as(name))
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
    DeconstructedNestedField(resultDf, newFieldName, transientColName)
  }

  /**
    * Renames a column `columnFrom` to `columnTo` replacing the original column and putting the resulting column
    * under the same struct level of nesting as `columnFrom`.
    *
    * @param inputDf    A dataframe to process
    * @param columnFrom A column name that needs to be put into a nested struct
    * @param columnTo   A column name that `columnFrom` should have after it is renamed
    * @param positionColumn A column that should be replaced by contents of columnFrom. It makrs the position of
    *                       the target column placement.
    * @return A transformed dataframe
    **/
  def nestedRenameReplace(inputDf: DataFrame,
                          columnFrom: String,
                          columnTo: String,
                          positionColumn: Option[String] = None): DataFrame = {
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
      putFieldIntoNestedStruct(inputDf, columnFrom, columnTo.split('.'), positionColumn)
    }
  }

  private def putFieldIntoNestedStruct(df: DataFrame,
                               columnFrom: String,
                               pathTo: Seq[String],
                               placementCol: Option[String] = None): DataFrame = {
    def processStruct(schema: StructType, path: Seq[String], parentCol: Option[Column]): Seq[Column] = {
      val currentField = path.head
      val isLeaf = path.lengthCompare(1) <= 0
      var isFound = false

      val newFields = schema.fields.flatMap(field => {
        if (field.name == columnFrom) {
          // This removes the original column name (if any) and the transient column
          Nil
        } else if (!isFound && isLeaf && placementCol.isDefined && placementCol.get == field.name) {
          isFound = true
          Seq(col(s"`$columnFrom`").as(currentField))
        } else if (!isFound && field.name == currentField) {
          field.dataType match {
            case _ if isLeaf =>
              isFound = true
              Seq(col(s"`$columnFrom`").as(currentField))
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
        val c = col(s"`$columnFrom`")
        newFields :+ c.as(currentField)
      } else {
        newFields
      }
    }

    df.select(processStruct(df.schema, pathTo, None): _*)
  }

  private def addSuperTransientField(inputDf: DataFrame, arrayColPathName: String): (DataFrame, String) = {
    val colName = SchemaUtils.getUniqueName(superTransientColumnName, Some(inputDf.schema))
    val nestedColName = (arrayColPathName.split('.').dropRight(1) :+ colName).mkString(".")
    val df = inputDf.nestedWithColumn(nestedColName, lit(null))
    (df, nestedColName)
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

  private val deconstructedColumnName = "electron"
  private val explosionTmpColumnName = "proton"
  private val nullRestoredTmpColumnName = "neutron"
  private val transientColumnName = "quark"
  private val superTransientColumnName = "higgs"
}
