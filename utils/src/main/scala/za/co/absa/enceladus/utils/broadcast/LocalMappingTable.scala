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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType}
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.collection.mutable.ListBuffer

/**
  * This class contains all necessary information to apply a mapping rule locally on executors.
  */
case class LocalMappingTable(
                              map: Map[Seq[Any], Any],
                              keyFields: Seq[String],
                              targetAttribute: String,
                              keyTypes: Seq[DataType],
                              valueType: DataType
                            )

object LocalMappingTable {

  /**
    * Creates a local mapping table from a mapping table dataframe.
    *
    * @param mappingTableDf  A mapping table dataframe.
    * @param keyFields       A list of dataframe columns to be used as mapping keys
    * @param targetAttribute A column to be used as the mapping value
    */
  @throws[IllegalArgumentException]
  def apply(mappingTableDf: DataFrame,
            keyFields: Seq[String],
            targetAttribute: String): LocalMappingTable = {

    validateKeyFields(mappingTableDf, keyFields)
    validateTargetAttribute(mappingTableDf, targetAttribute)

    val keyTypes = keyFields.map(fieldName =>
      SchemaUtils.getFieldType(fieldName, mappingTableDf.schema).get
    )

    val valueType = SchemaUtils.getFieldType(targetAttribute, mappingTableDf.schema).get

    val mappingColumns = col(targetAttribute) +: keyFields.map(c => col(c))
    val projectedDf = mappingTableDf.select(mappingColumns: _*)
    val numberOfKeys = keyFields.size

    val mappingTable = projectedDf.collect().map(row => {
      val value = row(0)
      val keys = new ListBuffer[Any]
      var i = 0
      while (i < numberOfKeys) {
        keys += row(i + 1)
        i += i + 1
      }
      (keys.toSeq, value)
    }).toMap

    LocalMappingTable(mappingTable, keyFields, targetAttribute, keyTypes, valueType)
  }

  private def validateKeyFields(mappingTableDf: DataFrame, keyFields: Seq[String]): Unit = {
    if (keyFields.isEmpty) {
      throw new IllegalArgumentException("No join key fields are provided for the mapping table.")
    }
    keyFields.foreach(field => {
      SchemaUtils.getFieldType(field, mappingTableDf.schema) match {
        case Some(_: ArrayType) => throw new IllegalArgumentException(s"Join condition field cannot be an array: $field.")
        case Some(_) =>
        case None => throw new IllegalArgumentException(s"Join condition field does not exist: $field.")
      }
    })

    keyFields.foreach(field => {
      val arraySubPath = SchemaUtils.getFirstArrayPath(field, mappingTableDf.schema)
      if (arraySubPath.nonEmpty) {
        throw new IllegalArgumentException(s"Join key field $field is inside an array $arraySubPath.")
      }
    })
  }

  private def validateTargetAttribute(mappingTableDf: DataFrame, targetAttribute: String): Unit = {
    SchemaUtils.getFieldType(targetAttribute, mappingTableDf.schema) match {
      case Some(_: ArrayType) => throw new IllegalArgumentException(s"Target attribute cannot be an array: $targetAttribute.")
      case Some(_) =>
      case None => throw new IllegalArgumentException(s"Target attribute $targetAttribute does not exist in the mapping table.")
    }
    val arraySubPath = SchemaUtils.getFirstArrayPath(targetAttribute, mappingTableDf.schema)
    if (arraySubPath.nonEmpty) {
      throw new IllegalArgumentException(s"Target attribute $targetAttribute is inside an array $arraySubPath.")
    }
  }

}

