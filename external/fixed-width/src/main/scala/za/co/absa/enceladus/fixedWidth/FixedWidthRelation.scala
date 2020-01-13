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

package za.co.absa.enceladus.fixedWidth

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.fixedWidth.types.SparkTypeResolver
import za.co.absa.enceladus.fixedWidth.parameters.FixedWidthParameters

class FixedWidthRelation(sourceDirPath: String, dataSchema: StructType, trimValues: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with Serializable
    with TableScan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def schema: StructType = {
    dataSchema
  }

  override def buildScan(): RDD[Row] = {
    val records = sqlContext.sparkSession.sparkContext.textFile(sourceDirPath)
    parseRecords(records).flatMap(row => row)
  }

  private def parseRecords(records: RDD[String]) = {
    if (records.isEmpty()) {
      throw new IllegalStateException(s"No data within the file: $sourceDirPath")
    }
    val schemaFields = schema.fields
    val indexedTriple = getFixedWidthArray(schemaFields)
    records.map(f = content => {
      val lines = content.split("\n")
      val rowCollection: Array[Row] = lines.map(line => {
        val rowElement = indexedTriple.map(triple => {
          val element = line.substring(triple._1, triple._2)
          SparkTypeResolver.toSparkType(triple._3, getProcessedValue(element))
        }
        )
        Row.fromSeq(rowElement)
      })
      rowCollection
    })
  }

  private def getFixedWidthArray(fields: Array[StructField]): Seq[(Int, Int, StructField)] = {
    var index = 0
    fields.map(field =>
      if (field.metadata.contains("width")) {
        val width = FixedWidthParameters.validateWidthValue(field, field.name)
        val length = index + width
        val triple: (Int, Int, StructField) = (index, index + width, field)
        index = length
        triple
      } else {
        throw new IllegalStateException(s"No width has been defined for the column ${field.name}")
      })
  }

  private def getProcessedValue(value: String): String = {
    if (!trimValues) {
      value
    } else {
      value.trim
    }
  }
}
