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

package za.co.absa.enceladus.utils.implicits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object EnceladusDataFrameImplicits {
  implicit class EnceladusDataframeEnhancements(val df: DataFrame) extends AnyVal {
    /**
      * Set nullable property of column.
      *
      * @param columnName is the column name to change
      * @param nullable   boolean flag to set the nullability of the column `columnName` to
      */
    def withNullableColumnState(columnName: String, nullable: Boolean): DataFrame = {
      // Courtesy of https://stackoverflow.com/a/33195510

      // modify [[StructField] with name `columnName` if its nullability differs
      val newSchema = StructType(df.schema.map {
        case StructField(c, t, n, m) if c.equals(columnName) && n != nullable =>
          StructField(c, t, nullable = nullable, m)
        case y: StructField => y
      })

      if (newSchema == df.schema) {
        df // no change
      } else {
        df.sqlContext.createDataFrame(df.rdd, newSchema) // apply new schema
      }
    }
  }
}
