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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error._
import za.co.absa.enceladus.utils.explode.{ExplodeTools, ExplosionContext}
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations.arrCol
import za.co.absa.enceladus.utils.udf.UDFNames
import za.co.absa.spark.hats.transformations.NestedArrayTransformations

case class MappingRuleInterpreterGroupExplode(rule: MappingConformanceRule,
                                              conformance: ConfDataset)
  extends RuleInterpreter with CommonMappingRuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  override def conform(df: DataFrame)
              (implicit spark: SparkSession,
               explosionState: ExplosionState,
               dao: MenasDAO,
               progArgs: InterpreterContextArgs): DataFrame = {
    log.info(s"Processing mapping rule to conform ${outputColumnNames()} (group explode strategy)...")

    val (mapTable, defaultValuesMap) = conformPreparation(df, enableCrossJoin = true)

    val (explodedDf, expCtx) = explodeIfNeeded(df, explosionState)

    val outputElements = multiRule.outputColumns.map { case (outputColumn: String, targetAttribute: String) =>
      col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.$targetAttribute") as outputColumn
    }.toSeq

    val outputsStructColumnName = getOutputsStructColumnName(df)
    val columns = Seq(col(s"${CommonMappingRuleInterpreter.inputDfAlias}.*"),
      struct(outputElements: _*) as outputsStructColumnName)

    val joined = explodedDf.as(CommonMappingRuleInterpreter.inputDfAlias)
      .join(mapTable.as(CommonMappingRuleInterpreter.mappingTableAlias), joinCondition, CommonMappingRuleInterpreter.joinType)
      .select(columns: _*)

    val mappings = rule.attributeMappings.map(x => Mapping(x._1, x._2)).toSeq
    val mappingErrUdfCall = callUDF(UDFNames.confMappingErr, lit(outputColumnNames()),
      array(rule.attributeMappings.values.toSeq.map(arrCol(_).cast(StringType)): _*),
      typedLit(mappings))

    val placedDf = ExplodeTools.nestedRenameReplace(joined, outputsStructColumnName, outputsStructColumnName)

    val arrayErrorCondition = getErrorCondition(expCtx, outputsStructColumnName)

    log.debug(s"Array Error Condition = $arrayErrorCondition")
    val withErrorsDf: DataFrame = addErrorsToErrCol(placedDf, outputsStructColumnName, mappingErrUdfCall, arrayErrorCondition)

    val flatOutputsWithDefaults = getFlatOutputsWithDefaults(outputsStructColumnName, defaultValuesMap)
    val allOtherColumns = withErrorsDf.columns.filter(! _.contains(outputsStructColumnName)).map(col).toSeq

    val flattenedDefaults = withErrorsDf.select(flatOutputsWithDefaults ++ allOtherColumns: _*)
    collectIfNeeded(expCtx, explosionState, flattenedDefaults)
  }

  private def getFlatOutputsWithDefaults(outputsStructColumnName: String, defaultValuesMap: Map[String, String]) = {
    multiRule.outputColumns.keys.map(colName => {
      val column = col(outputsStructColumnName + "." + colName)
      defaultValuesMap.get(colName) match {
        case Some(defValue) => when(column.isNotNull, column).otherwise(expr(defValue))
        case None => column
      }
    }).toSeq
  }

  private def getErrorCondition(expCtx: ExplosionContext, outputsStructColumnName: String) = {
    multiRule.outputColumns.keys.foldLeft(lit(false))((acc: Column, nestedColumn: String) => {
      val nestedOutputName = s"${outputsStructColumnName}.$nestedColumn"
      val nestedFieldCondition = col(nestedOutputName).isNull.and(expCtx.getArrayErrorCondition(nestedOutputName))
      acc.or(nestedFieldCondition)
    })
  }

  override protected def validateMappingFieldsExist(joinConditionStr: String,
                                                    datasetSchema: StructType,
                                                    mappingTableSchema: StructType,
                                                    rule: MappingConformanceRule): Unit = {
    logJoinCondition(mappingTableSchema, joinConditionStr)
    // validate join fields existence
    super.validateMappingFieldsExist(joinConditionStr, datasetSchema, mappingTableSchema, rule)
  }

  private def explodeIfNeeded(df: Dataset[Row], explosionState: ExplosionState): (Dataset[Row], ExplosionContext) = {
    if (explosionState.explodeContext.explosions.isEmpty) {
      ExplodeTools.explodeAllArraysInPath(rule.outputColumn, df)
    } else {
      (df, explosionState.explodeContext)
    }
  }

  private def collectIfNeeded(expCtx: ExplosionContext, explosionState: ExplosionState, errorsDf: DataFrame): DataFrame = {
    if (explosionState.explodeContext.explosions.isEmpty) {
      ExplodeTools.revertAllExplosions(errorsDf, expCtx, Some(ErrorMessage.errorColumnName))
    } else {
      errorsDf
    }
  }

  private def addErrorsToErrCol(df: DataFrame,
                                outputsStructColumnName: String,
                                mappingErrUdfCall: Column,
                                errorConditions: Column): DataFrame = {
    NestedArrayTransformations.nestedWithColumnAndErrorMap(df, outputsStructColumnName, outputsStructColumnName,
      ErrorMessage.errorColumnName, c => c, _ => {
        when(errorConditions, mappingErrUdfCall).otherwise(null) // scalastyle:ignore null
      }
    )
  }

  private def logJoinCondition(mappingTableSchema: StructType, joinConditionStr: String): Unit = {
    log.info(s"Mapping table: \n${mappingTableSchema.treeString}")
    log.info(s"Rule: ${this.toString}")
    log.info(s"Join Condition: $joinConditionStr")
  }
}
