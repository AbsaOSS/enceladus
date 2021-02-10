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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
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
    val (mapTable, defaultValueOpt) = conformPreparation(df, enableCrossJoin = false)
    val mappingTableFields = multiRule.attributeMappings.keys.toSeq
    val inputDfFields = multiRule.attributeMappings.values.toSeq

    val mt = LocalMappingTable(mapTable, mappingTableFields, multiRule.outputColumns.values.toSeq)
    val broadcastedMt = spark.sparkContext.broadcast(mt)
    val mappingUDF = BroadcastUtils.getMappingUdf(broadcastedMt, defaultValueOpt)

    val projectedCols = multiRule.outputColumns.keys.map(col).toSeq
    val outputsProjected = df.withColumn("outputs", mappingUDF(projectedCols: _*))
      .select("outputs.*")

    multiRule.outputColumns.foldLeft(outputsProjected)((prevDf: DataFrame, outputCol: (String, String)) => {
      val parentPath = getParentPath(outputCol._1)
      val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, outputCol._1, mappings)
      NestedArrayTransformations.nestedAddColumnExtended(prevDf, outputCol._1, (_, getField) => {
        errorUDF(inputDfFields.map(a => getField(a)): _ *)
      })

      /*NestedArrayTransformations.nestedExtendedStructAndErrorMap(
        prevDf, parentPath, outputCol._1, ErrorMessage.errorColumnName, (_, getField: GetFieldFunction) => {
          mappingUDF(inputDfFields.map(a => getField(a)): _ *)
        }, (_, getField) => {
          errorUDF(inputDfFields.map(a => getField(a)): _ *)
        })*/
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


