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

package za.co.absa.enceladus.conformance.interpreter.rules.mapping

import org.apache.spark.sql.functions.{array, col, expr, flatten, struct, when}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.broadcast.{BroadcastUtils, LocalMappingTable}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.spark.hats.transformations.NestedArrayTransformations
import za.co.absa.spark.hats.transformations.NestedArrayTransformations.GetFieldFunction

case class MappingRuleInterpreterBroadcast(rule: MappingConformanceRule, conformance: ConfDataset)
  extends RuleInterpreter with CommonMappingRuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  override def conform(df: DataFrame)
                      (implicit spark: SparkSession,
                       explosionState: ExplosionState,
                       dao: MenasDAO,
                       progArgs: InterpreterContextArgs): DataFrame = {
    log.info(s"Processing mapping rule to conform ${outputColumnNames()} (broadcast strategy)...")
    val (mapTable, defaultValuesMap) = conformPreparation(df, enableCrossJoin = false)
    val mappingTableFields = rule.attributeMappings.keys.toSeq
    val inputDfFields = rule.attributeMappings.values.toSeq

    val parentPath = getParentPath(rule.outputColumn)

    if (rule.additionalColumns.isEmpty) {
      val mt = LocalMappingTable(mapTable, mappingTableFields, Seq(rule.targetAttribute))
      val broadcastedMt = spark.sparkContext.broadcast(mt)
      val mappingUDF = BroadcastUtils.getMappingUdfForSingleOutput(broadcastedMt, defaultValuesMap.get(rule.targetAttribute))

      val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, Seq(rule.outputColumn), mappings)

      val withMappedFieldsDf = NestedArrayTransformations.nestedExtendedStructAndErrorMap(
        df, parentPath, rule.outputColumn, ErrorMessage.errorColumnName, (_, getField) => {
          mappingUDF(inputDfFields.map(a => getField(a)): _ *)
        }, (_, getField) => {
          errorUDF(inputDfFields.map(a => getField(a)): _ *)
        })

      withMappedFieldsDf
    } else {
      val mt = LocalMappingTable(mapTable, mappingTableFields, rule.allOutputColumns().values.toSeq)
      val broadcastedMt = spark.sparkContext.broadcast(mt)
      val mappingUDF = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt)

      val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, rule.allOutputColumns().keys.toSeq, mappings)

      val outputsStructColumnName = getOutputsStructColumnName(df)
      val outputsStructColumn = if(parentPath == "") outputsStructColumnName else parentPath + "." + outputsStructColumnName
      val frame = NestedArrayTransformations.nestedExtendedStructAndErrorMap(
        df, parentPath, outputsStructColumn, ErrorMessage.errorColumnName, (_, getField: GetFieldFunction) => {
          mappingUDF(inputDfFields.map(a => getField(a)): _ *)
        }, (_, getField) => {
          errorUDF(inputDfFields.map(a => getField(a)): _ *)
        })

      //TODO Try multiple applications for udf -> PR

      if (parentPath == ""){
        flattenOutputsStructInFlatParent(outputsStructColumn, frame, defaultValuesMap)
      }
      else {
        flattenOutputsStructInNestedParent(parentPath, outputsStructColumnName, frame, defaultValuesMap)
      }
    }
  }

  private def flattenOutputsStructInFlatParent(outputsStructColumnName: String, frame: DataFrame,
                                               defaultMappingValues: Map[String, String]) = {
    val flattenedColumns = rule.allOutputColumns().map{
      case (outputColumn, targetAttribute) => {
        val fieldInOutputs = col(outputsStructColumnName + "." + targetAttribute)
        defaultMappingValues.get(targetAttribute) match {
          case Some(defValue) => when(fieldInOutputs.isNotNull, fieldInOutputs as outputColumn)
            .otherwise(expr(defValue)) as outputColumn
          case None => fieldInOutputs as outputColumn
        }
      }}.toSeq ++
      frame.columns.filter(!_.contains(outputsStructColumnName)).map(col).toSeq
    frame.select(flattenedColumns: _*)
  }

  private def flattenOutputsStructInNestedParent(parentPath: String,
                                                 outputsColumnName: String,
                                                 df: DataFrame,
                                                 defaultMappingValues: Map[String, String]) = {
    val dff = if (parentPath.contains(".")) df.select(s"${getParentPath(parentPath)}.*") else df
    val otherStructFields = dff.schema.filter(c => c.name == parentPath)
      .flatMap(a => {
        a.dataType match {
          case ArrayType(elementType, _) => elementType.asInstanceOf[StructType].fields
          case StructType(fields) => fields
        }
      })
      .filter(_.name != outputsColumnName)
      .map(sF => col(sF.name)).toSeq

    NestedArrayTransformations.nestedWithColumnMap(df, parentPath, parentPath, column => {
      val flattenedOutputs = rule.allOutputColumns().map { case (outputName, targetAttribute) =>
        val newOutputColName = if (outputName.contains(".")) outputName.split("\\.").last else outputName
        val fieldInOutputs = column.getField(outputsColumnName).getField(targetAttribute)
        defaultMappingValues.get(targetAttribute) match {
          case Some(defValue) => when(fieldInOutputs.isNotNull, fieldInOutputs as newOutputColName).otherwise(expr(defValue))
          case None => fieldInOutputs as newOutputColName
        }
      }.toSeq
      val columns = otherStructFields ++ flattenedOutputs
      struct(columns: _*).as(parentPath)
    })
  }

}


