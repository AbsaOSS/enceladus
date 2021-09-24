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

package za.co.absa.enceladus.model

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import za.co.absa.enceladus.model.conformanceRule._

case class ConformedSchema(schema: List[StructField], dataset: Dataset) {
  def hasField(field: String): Boolean = {
    if (schema.exists(_.name == field)) true else {
      val ss = dataset.conformance.find {
        case UppercaseConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case LiteralConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case NegationConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case CastingConformanceRule(_, outputColumn, _, _, _) => outputColumn == field
        case MappingConformanceRule(_, _, _, _, _, _, outputColumn, additionalColumns, _, _, _) =>
          outputColumn == field || additionalColumns.getOrElse(Map()).contains(field)
        case SingleColumnConformanceRule(_, _, outputColumn, _, _) => outputColumn == field
        case DropConformanceRule(_, _, outputColumn) => outputColumn != field
        case ConcatenationConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case CoalesceConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case FillNullsConformanceRule(_, outputColumn, _, _, _) => outputColumn == field
        case SparkSessionConfConformanceRule(_, outputColumn, _, _) => outputColumn == field
        case _ => false
      }
      ss.isDefined
    }
  }
}

object ConformedSchema {

  def create(dataset: Dataset, structFields: Array[StructField],
            mappingTableSchemas: Map[String, Array[StructField]])(implicit spark: SparkSession): ConformedSchema = {
    import spark.implicits._

    val fields: List[StructField] = dataset.conformance.foldLeft(structFields.toList)((acc: List[StructField],
                                                                                       conformanceRule: ConformanceRule) => {
      conformanceRule match {
        case UppercaseConformanceRule(_, outputColumn, _, inputColumn) => {
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumn, outputColumn, structFields))
        }
        case LiteralConformanceRule(_, outputColumn, _, value) => {
          val fieldType = List(1).toDF().withColumn("dummy", expr(value)).schema.fields(1).dataType
          acc ++ List(StructField(outputColumn, fieldType))
        }
        case NegationConformanceRule(_, outputColumn, _, inputColumn) =>
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumn, outputColumn, structFields))
        case CastingConformanceRule(_, outputColumn, _, inputColumn, outputDataType: String) =>
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumn, outputColumn, structFields)
            .copy(dataType = DataType.fromDDL(outputDataType)))
        case MappingConformanceRule(_, _, mappingTable, _, _, targetAttribute, outputColumn, additionalColumns, _, _, _) =>
          val mappingTableSchemaFields: Array[StructField] = mappingTableSchemas(mappingTable)
          val fieldsMapping = additionalColumns.getOrElse(Map()) ++ Map(outputColumn -> targetAttribute)
          val mappingFeilds = fieldsMapping.map{case (outputCol, targetAttribute) =>
            getProjectedSchemaFieldFromStructField(targetAttribute, outputCol, mappingTableSchemaFields)
          }.toList
          acc ++ mappingFeilds
        case SingleColumnConformanceRule(_, _, outputColumn, inputColumn, inputColumnAlias) =>
          StructField(outputColumn, StructType(Seq(StructField(inputColumnAlias, StringType))))
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumn, outputColumn, structFields))
        case DropConformanceRule(_, _, outputColumn) => acc.filterNot(_.name == outputColumn)
        case ConcatenationConformanceRule(_, outputColumn, _, inputColumns) =>
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumns.head, outputColumn, structFields))
        case CoalesceConformanceRule(_, outputColumn, _, inputColumns) =>
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumns.head, outputColumn, structFields))
        case FillNullsConformanceRule(_, outputColumn, _, inputColumn, value) =>
          acc ++ List(getProjectedSchemaFieldFromStructField(inputColumn, outputColumn, structFields))
        case SparkSessionConfConformanceRule(_, outputColumn, _, sparkConfKey) => {
          spark.sparkContext.getConf.get(sparkConfKey)
          acc ++ List(StructField(name= outputColumn, StringType))
        }
        case _ => acc
      }

    })
    ConformedSchema(fields, dataset)
  }

  def getProjectedSchemaFieldFromStructField(inputCol: String, outputCol: String, structFields: Array[StructField]): StructField = {
    val value = structFields.find(_.name == inputCol).get
    value.copy(name = outputCol)
  }
}


