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

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import za.co.absa.spark.commons.implicits.StructTypeImplicits.{StructTypeEnhancementsArrays}

/**
  * This class contains all necessary information to apply a mapping rule locally on executors.
  */
final case class LocalMappingTable private(
                                    private val data: Map[Seq[Any], Any],
                                    outputColumns: Map[String, String],
                                    keyTypes: Seq[DataType],
                                    valueTypes: Seq[DataType]
                                  ) {

  def getRowWithDefault(key: Seq[Any], default: Any): Any = {
    data.getOrElse(key, default)
  }

  def contains(key: Seq[Any]): Boolean = {
    data.contains(key)
  }

  def rowCount: Int = data.size
}

object LocalMappingTable {

  /**
    * Creates a local mapping table from a mapping table dataframe.
    *
    * @param mappingTableDf   A mapping table dataframe.
    * @param keyFields        A list of dataframe columns to be used as mapping keys
    * @param outputColumns    A map of conformed column names and target attributes to be used as the mapping values
    */
  @throws[IllegalArgumentException]
  def apply(mappingTableDf: DataFrame,
            keyFields: Seq[String],
            outputColumns: Map[String, String]
           ): LocalMappingTable = {

    validateKeyFields(mappingTableDf, keyFields)
    val targetAttributes = outputColumns.values.toSeq
    validateTargetAttributes(mappingTableDf, targetAttributes)

    val keyTypes = keyFields.flatMap(fieldName => mappingTableDf.schema.getFieldType(fieldName))

    val valueTypes = targetAttributes.flatMap(targetAttribute => {
      mappingTableDf.schema.getFieldType(targetAttribute)
    })
    val structFields: Seq[StructField] = outputColumns.keys.toSeq.zip(valueTypes)
      .map { case (name: String, fieldType: DataType) => StructField(name, fieldType) }
    val rowSchema = StructType(structFields)

    val targetColumns: Seq[Column] = targetAttributes.map(targetAttribute => col(targetAttribute))

    val mappingColumns = targetColumns ++ keyFields.map(c => col(c))
    val projectedDf = mappingTableDf.select(mappingColumns: _*)
    val numberOfKeys = keyFields.size
    val numberOfValues = targetAttributes.size

    val mappingTable = projectedDf.collect().map(row => {
      val values = (0 until numberOfValues).toArray map (row(_))
      val keys: Seq[Any] = (numberOfValues until numberOfValues + numberOfKeys) map(row(_))
      (keys, if (values.length == 1) values.head else new GenericRowWithSchema(values, rowSchema))
    }).toMap

    LocalMappingTable(mappingTable, outputColumns, keyTypes, valueTypes)
  }

  private def validateKeyFields(mappingTableDf: DataFrame, keyFields: Seq[String]): Unit = {
    keyFields.foreach(field => {
      mappingTableDf.schema.getFieldType(field) match {
        case Some(_: ArrayType) => throw new IllegalArgumentException(s"Join condition field cannot be an array: $field.")
        case Some(_: StructType) => throw new IllegalArgumentException(s"Join condition field cannot be a struct: $field.")
        case Some(_) =>
        case None => throw new IllegalArgumentException(s"Join condition field does not exist: $field.")
      }
    })

    keyFields.foreach(field => {
      val arraySubPath = mappingTableDf.schema.getFirstArrayPath(field)
      if (arraySubPath.nonEmpty) {
        throw new IllegalArgumentException(s"Join key field $field is inside an array $arraySubPath.")
      }
    })
  }

  private def validateTargetAttributes(mappingTableDf: DataFrame, targetAttributes: Seq[String]): Unit = {
    targetAttributes.foreach(targetAttribute => {
      mappingTableDf.schema.getFieldType(targetAttribute) match {
        case Some(_: ArrayType) => throw new IllegalArgumentException(s"Target attribute cannot be an array: $targetAttribute.")
        case Some(_) =>
        case None => throw new IllegalArgumentException(s"Target attribute $targetAttribute does not exist in the mapping table.")
      }
      val arraySubPath = mappingTableDf.schema.getFirstArrayPath(targetAttribute)
      if (arraySubPath.nonEmpty) {
        throw new IllegalArgumentException(s"Target attribute $targetAttribute is inside an array $arraySubPath.")
      }
    })
  }

}

