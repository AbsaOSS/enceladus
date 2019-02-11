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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import za.co.absa.enceladus.utils.schema.SchemaUtils._

object ExplodeTools {
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
    val explodedIdName = getClosestUniqueName(s"${arrayFieldName}_id", df.schema)
    val explodedIndexName = getClosestUniqueName(s"${arrayFieldName}_idx", df.schema)
    val explodedSizeName = getClosestUniqueName(s"${arrayFieldName}_size", df.schema)

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
      .drop(arrayFieldName)
      .withColumnRenamed(explodedColumnName, arrayFieldName)

    val newExplosion = Explosion(arrayFieldName, explodedIdName, explodedIndexName, explodedSizeName)
    val newContext = explosionContext.copy(explosions = newExplosion +: explosionContext.explosions)
    (explodedDf, newContext)
  }

  /**
    * Reverts all explosions done by explode array().
    * Context can be used to revert all explosions back
    */
  def revertAllExplosions(inputDf: DataFrame, explosionContext: ExplodeContext): DataFrame = {
    explosionContext.explosions.foldLeft(inputDf) ( (df, explosion) => {
      revertSingleExplosion(df, explosion)
    })
  }

  /**
    * Reverts aa particular explode made by explodeArray().
    * If there were several explodes they should be reverted in FILO order
    */
  def revertSingleExplosion(df: DataFrame, explosion: Explosion): DataFrame = {
    val orderByCol = col(explosion.indexFieldName)
    val groupedCol = col(explosion.idFieldName)
    val structCol = col(explosion.arrayFieldName)

    // Do not group by columns that are explosion artifacts
    val allOtherColumns = df.schema
      .filter(a => a.name != explosion.idFieldName
        && a.name != explosion.indexFieldName
        && a.name != explosion.arrayFieldName
      )
      .map(a => col(a.name))

    val tmpColName = getUniqueName("tmp", Some(df.schema))

    // Implode
    df.orderBy(orderByCol).groupBy(groupedCol +: allOtherColumns: _*).agg(collect_list(structCol). as(tmpColName))
      // Drop original struct
      .drop(explosion.arrayFieldName)
      // restore null arrays
      .withColumn(explosion.arrayFieldName, when(col(explosion.sizeFieldName)>=0, col(tmpColName)).otherwise(null))
      // Drop the temporary column
      .drop(col(tmpColName))
      // Drop the array size column
      .drop(col(explosion.sizeFieldName))
      // restore original record order
      .orderBy(groupedCol)
      // remove monotonic id created during explode
      .drop(groupedCol)
  }

}
