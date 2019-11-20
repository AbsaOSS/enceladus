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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.broadcast.{BroadcastUtils, LocalMappingTable}
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations

case class MappingRuleInterpreterBroadcast(rule: MappingConformanceRule, conformance: ConfDataset) extends RuleInterpreter {

  private val conf = ConfigFactory.load()

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: MenasDAO, progArgs: CmdConfig): Dataset[Row] = {
    log.info(s"Processing mapping rule to conform ${rule.outputColumn} (broadcast strategy)...")

    val datasetSchema = dao.getSchema(conformance.schemaName, conformance.schemaVersion)

    val mapPartitioning = conf.getString("conformance.mappingtable.pattern")
    val mappingTableDef = dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)

    // find the data frame from the mapping table
    val mapTable = DataSource.getData(mappingTableDef.hdfsPath, progArgs.reportDate, mapPartitioning)

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
    val mappingUDF = BroadcastUtils.getMappingUdf(broadcastedMt)

    val errorUDF = BroadcastUtils.getErrorUdf(broadcastedMt, rule.outputColumn, mappings)

    val tempErrorColName = SchemaUtils.getUniqueName("err", Some(df.schema))

    val res1 = df.withColumn(rule.outputColumn, mappingUDF(inputDfFields.map(a => col(a)): _ *))
      .withColumn(rule.outputColumn, mappingUDF(inputDfFields.map(a => col(a)): _ *))
      .withColumn(tempErrorColName, array(errorUDF(inputDfFields.map(a => col(a)): _ *)))

    DeepArrayTransformations.gatherErrors(res1, tempErrorColName, ErrorMessage.errorColumnName)
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


