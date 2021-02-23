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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import za.co.absa.enceladus.utils.schema.SchemaUtils

import scala.collection.mutable.ListBuffer

/**
  * This class contains all necessary information to apply a mapping rule locally on executors.
  */
final case class LocalMappingTable(
                                    map: Map[Seq[Any], Any],
                                    keyFields: Seq[String],
                                    targetAttributes: Seq[String],
                                    keyTypes: Seq[DataType],
                                    valueTypes: Seq[DataType]
                                  )

object LocalMappingTable {

  /**
    * Creates a local mapping table from a mapping table dataframe.
    *
    * @param mappingTableDf   A mapping table dataframe.
    * @param keyFields        A list of dataframe columns to be used as mapping keys
    * @param targetAttributes A list of column names to be used as the mapping values
    */
  @throws[IllegalArgumentException]
  def apply(mappingTableDf: DataFrame,
            keyFields: Seq[String],
            targetAttributes: Seq[String]): LocalMappingTable = {

    validateKeyFields(mappingTableDf, keyFields)
    validateTargetAttributes(mappingTableDf, targetAttributes)

    val keyTypes = keyFields.map(fieldName =>
      SchemaUtils.getFieldType(fieldName, mappingTableDf.schema).get
    )

    val valueTypes = targetAttributes.map(targetAttribute => {
      SchemaUtils.getFieldType(targetAttribute, mappingTableDf.schema).get
    })

    val targetColumns: Seq[Column] = targetAttributes.map(targetAttribute => col(targetAttribute))

    val mappingColumns = targetColumns ++ keyFields.map(c => col(c))
    val projectedDf = mappingTableDf.select(mappingColumns: _*)
    val numberOfKeys = keyFields.size
    val numberOfValues = targetAttributes.size

    val mappingTable = projectedDf.collect().map(row => {
      var i = 0
      val values = new ListBuffer[Any]
      while (i < numberOfValues) {
        values += row(i)
        i += 1
      }

      val keys = new ListBuffer[Any]
      while (i < numberOfKeys + numberOfValues) {
        keys += row(i)
        i += 1
      }
      (keys.toSeq, if (values.size == 1) values.head else Row.fromSeq(values.toList))
    }).toMap

    LocalMappingTable(mappingTable, keyFields, targetAttributes, keyTypes, valueTypes)
  }

  private def validateKeyFields(mappingTableDf: DataFrame, keyFields: Seq[String]): Unit = {
    if (keyFields.isEmpty) {
      throw new IllegalArgumentException("No join key fields are provided for the mapping table.")
    }
    keyFields.foreach(field => {
      SchemaUtils.getFieldType(field, mappingTableDf.schema) match {
        case Some(_: ArrayType) => throw new IllegalArgumentException(s"Join condition field cannot be an array: $field.")
        case Some(_: StructType) => throw new IllegalArgumentException(s"Join condition field cannot be a struct: $field.")
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

  private def validateTargetAttributes(mappingTableDf: DataFrame, targetAttributes: Seq[String]): Unit = {
    targetAttributes.foreach(targetAttribute => {
      SchemaUtils.getFieldType(targetAttribute, mappingTableDf.schema) match {
        case Some(_: ArrayType) => throw new IllegalArgumentException(s"Target attribute cannot be an array: $targetAttribute.")
        case Some(_) =>
        case None => throw new IllegalArgumentException(s"Target attribute $targetAttribute does not exist in the mapping table.")
      }
      val arraySubPath = SchemaUtils.getFirstArrayPath(targetAttribute, mappingTableDf.schema)
      if (arraySubPath.nonEmpty) {
        throw new IllegalArgumentException(s"Target attribute $targetAttribute is inside an array $arraySubPath.")
      }
    })
  }

}

