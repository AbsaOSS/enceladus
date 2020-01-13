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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Stores a context of several array explosions to they can be reverted in the proper order.
  */
case class ExplosionContext(explosions: Seq[Explosion] = Nil) {

  /** Given a column name generates a condition that should hold if an error is to be generated
    * when joining against any array in the column path */
  def getArrayErrorCondition(columnName: String): Column = {
    explosions.foldLeft(lit(true))((expr, explosion) => {
      if (columnName.startsWith(explosion.arrayFieldName)) {
        val arrayIsNull = col(explosion.sizeFieldName) === lit(-1)
        val arrayIsEmpty = col(explosion.sizeFieldName) === lit(0)
        expr and !arrayIsEmpty and !arrayIsNull
      } else {
        expr
      }
    })
  }


  /** Generates a condition filter for the exploded dataset so control measurements can
    * be used for non-array elements. */
  def getControlFrameworkFilter: Column = {
    explosions.foldLeft(lit(true))((cond, explosion) => {
      cond.and(coalesce(col(explosion.indexFieldName), lit(0)) === 0)
    })
  }

}
