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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.conformance.interpreter.ExplosionState
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.broadcast.{BroadcastUtils, LocalMappingTable}
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations

case class MappingRuleInterpreterBroadcast(rule: MappingConformanceRule, conformance: ConfDataset) extends RuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: CmdConfig): Dataset[Row] = {
    log.info(s"Processing mapping rule to conform ${rule.outputColumn} (broadcast strategy)...")

    val mappingTableDef = dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)

    // find the data frame from the mapping table
    val mapTable = DataSource.getDataFrame(mappingTableDef.hdfsPath, progArgs.reportDate)

    // join & perform projection on the target attribute
    val joinConditionStr = MappingRuleInterpreter.getJoinCondition(rule).toString
    log.info("Mapping table: \n" + mapTable.schema.treeString)
    log.info("Rule: " + this.toString)
    log.info("Join Condition: " + joinConditionStr)

    // validate the default value against the mapping table schema
    val defaultValueOpt = getDefaultValue(mappingTableDef)

    val mappingTableFields = rule.attributeMappings.keys.toSeq
    val inputDfFields = rule.attributeMappings.values.toSeq
    val mappings = rule.attributeMappings.map {
      case (mappingTableField, dataframeField) => Mapping(mappingTableField, dataframeField)
    }.toSeq

    val mt = LocalMappingTable(mapTable, mappingTableFields, rule.targetAttribute)
    val broadcastedMt = spark.sparkContext.broadcast(mt)
    val mappingUDF = BroadcastUtils.getMappingUdf(broadcastedMt, defaultValueOpt)

    val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, rule.outputColumn, mappings)

    val arrayPath = splitArrayPath(df.schema, inputDfFields.head)._1
    val keySubPaths = inputDfFields.map(colNam => splitArrayPath(df.schema, colNam)._2)
    val outputSubPath = splitArrayPath(df.schema, rule.outputColumn)._2

    val withMappedFieldsDf = DeepArrayTransformations.nestedStructAndErrorMap(
      df, arrayPath, outputSubPath, ErrorMessage.errorColumnName, structCol => {
        if (structCol == null) {
          mappingUDF(keySubPaths.map(a => col(a)): _ *)
        } else {
          mappingUDF(keySubPaths.map(a => structCol.getField(a)): _ *)
        }
      }, structCol => {
        if (structCol == null) {
          errorUDF(keySubPaths.map(a => col(a)): _ *)
        } else {
          errorUDF(keySubPaths.map(a => structCol.getField(a)): _ *)
        }
      })

    withMappedFieldsDf
  }

  /**
    * Splits a fully qualified column name into a deepest array subpath and the
    * rest of the path.
    *
    * @param schema     The schema of a DataFrame
    * @param columnName A fully qualified column name
    */
  private def splitArrayPath(schema: StructType, columnName: String): (String, String) = {
    val arraySubPathOpt = SchemaUtils.getDeepestArrayPath(schema, columnName)

    arraySubPathOpt match {
      case Some(arraySubPath) => (arraySubPath, columnName.drop(arraySubPath.length + 1))
      case None => ("", columnName)
    }
  }

  private def getDefaultValue(mappingTableDef: MappingTable)
                             (implicit spark: SparkSession, dao: MenasDAO): Option[String] = {
    val defaultMappingValueMap = mappingTableDef.getDefaultMappingValues

    val attributeDefaultValueOpt = defaultMappingValueMap.get(rule.targetAttribute)
    val genericDefaultValueOpt = defaultMappingValueMap.get("*")

    val defaultValueOpt = attributeDefaultValueOpt match {
      case Some(_) => attributeDefaultValueOpt
      case None => genericDefaultValueOpt
    }

    if (defaultValueOpt.isDefined) {
      val mappingTableSchemaOpt = Option(dao.getSchema(mappingTableDef.schemaName, mappingTableDef.schemaVersion))
      mappingTableSchemaOpt match {
        case Some(schema) =>
          MappingRuleInterpreter.ensureDefaultValueMatchSchema(mappingTableDef.name, schema,
            rule.targetAttribute, defaultValueOpt.get)
        case None =>
          log.warn("Mapping table schema loading failed")
      }
    }
    defaultValueOpt
  }

}


