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

import org.apache.spark.sql.functions.{col, struct}
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
    val (mapTable, defaultValueOpt) = conformPreparation(df, enableCrossJoin = false)
    val mappingTableFields = multiRule.attributeMappings.keys.toSeq
    val inputDfFields = multiRule.attributeMappings.values.toSeq

    val mt = LocalMappingTable(mapTable, mappingTableFields, multiRule.outputColumns.values.toSeq)
    val broadcastedMt = spark.sparkContext.broadcast(mt)
    val mappingUDF = BroadcastUtils.getMappingUdf(broadcastedMt, defaultValueOpt)

    val parentPath = getParentPath(rule.outputColumn)
    val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, multiRule.outputColumns.keys.toSeq, mappings)

    val outputsStructColumnName = getOutputsStructColumnName(df)
    val frame = NestedArrayTransformations.nestedExtendedWithColumnAndErrorMap(
      df, parentPath, outputsStructColumnName, ErrorMessage.errorColumnName, (_, getField: GetFieldFunction) => {
        mappingUDF(inputDfFields.map(a => getField(a)): _ *)
      }, (_, getField) => {
        errorUDF(inputDfFields.map(a => getField(a)): _ *)
      })

    //TODO 1.keep implementation version for one output
    //TODO 2 Defaults
    //TODO 2.a) validate outputs are on the same level for now
    //TODO 3.try struct first, then think about other ways -> PR
    //TODO 4.try multiple applications for udf -> PR


    if (parentPath == ""){
      flattenOutputsStructInFlatParent(outputsStructColumnName, frame)
    }
    else {
      flattenOutputsStructInNestedParent(parentPath, outputsStructColumnName, frame, inputDfFields)
    }
  }

  private def flattenOutputsStructInFlatParent(outputsStructColumnName: String, frame: DataFrame) = {
    val flattenedColumns = multiRule.outputColumns.map{
      case (outputColumn, targetAttribute) => col(outputsStructColumnName + "." + targetAttribute) as outputColumn}.toSeq ++
      frame.columns.filter(!_.contains(outputsStructColumnName)).map(col).toSeq
    frame.select(flattenedColumns: _*)
  }

  private def flattenOutputsStructInNestedParent(parentPath: String, outputsStructColumnName: String, frame: DataFrame,
                                                 inputsDfString: Seq[String]) = {
    NestedArrayTransformations.nestedExtendedStructMap(frame, outputsStructColumnName, parentPath, (_, getField) => {
      val attributes: Seq[Column] = inputsDfString.map(a => getField(parentPath + "." + a))
      val flattenedOutputs = multiRule.outputColumns.map { case (outputName, targetAttribute) =>
        col(parentPath + "." + outputsStructColumnName + "." + targetAttribute) as parentPath + "." + outputName
      }
      struct(attributes ++ flattenedOutputs: _*)
    })
  }

  /**
    * Returns the parent path of a field. Returns an empty string if a root level field name is provided.
    *
    * @param columnName A fully qualified column name
    * @return The parent column name or an empty string if the input column is a root level column
    */
  private def getParentPath(columnName: String): String = {
    if (columnName.contains (".")) {
      columnName.split('.').dropRight(1).mkString(".")
    } else {
      ""
    }
  }

}


