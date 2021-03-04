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

import org.apache.spark.sql.{DataFrame, SparkSession}
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
      val mt = LocalMappingTable(mapTable, mappingTableFields, rule.allOutputColumns(), defaultValuesMap)
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
      val mt = LocalMappingTable(mapTable, mappingTableFields, rule.allOutputColumns(), defaultValuesMap)
      val broadcastedMt = spark.sparkContext.broadcast(mt)
      val mappingUDF = BroadcastUtils.getMappingUdfForMultipleOutputs(broadcastedMt)

      val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, rule.allOutputColumns().keys.toSeq, mappings)

      val outputsStructColumnName = getOutputsStructColumnName(df)
      val outputsStructColumn = if (parentPath == "") outputsStructColumnName else parentPath + "." + outputsStructColumnName
      val dfWithOutputCol = NestedArrayTransformations.nestedExtendedStructAndErrorMap(
        df, parentPath, outputsStructColumn, ErrorMessage.errorColumnName, (_, getField: GetFieldFunction) => {
          mappingUDF(inputDfFields.map(a => getField(a)): _ *)
        }, (_, getField) => {
          errorUDF(inputDfFields.map(a => getField(a)): _ *)
        })

      //TODO Try multiple applications for udf -> PR
      NestedArrayTransformations.nestedUnstruct(dfWithOutputCol, outputsStructColumn)
    }
  }
}


