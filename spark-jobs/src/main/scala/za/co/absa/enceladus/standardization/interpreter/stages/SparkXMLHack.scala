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

package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.enceladus.utils.transformations.ArrayTransformations.arrCol

/**
 * Hack around spark-xml bug: Null arrays produce array(null) instead of null.
 *
 * Get rid of this as soon as this is fixed in spark-xml
 */

object SparkXMLHack {

  def hack(field: StructField, path: String, df: Dataset[Row])(implicit spark: SparkSession, udfLib: UDFLibrary): Column = {
    val currentAttrPath = appendPath(path, field.name)

    field.dataType match {
      case a @ ArrayType(elType, nullable) =>
        when((size(arrCol(currentAttrPath)) === 1) and arrCol(currentAttrPath)(0).isNull, lit(null)).otherwise(arrCol(currentAttrPath)) as field.name // scalastyle:ignore null
      case t: StructType =>
        struct(t.fields.toSeq.map(x => hack(x, currentAttrPath, df)): _*) as field.name
      case _ =>
        arrCol(currentAttrPath) as field.name
    }
  }
}
