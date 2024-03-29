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

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.slf4j.Logger
import za.co.absa.spark.commons.utils.SchemaUtils
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.MappingTable
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter
import za.co.absa.enceladus.conformance.interpreter.rules.ValidationException
import za.co.absa.enceladus.utils.error.Mapping
import za.co.absa.enceladus.utils.validation.ExpressionValidator
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements

import scala.util.Try
import scala.util.control.NonFatal

trait CommonMappingRuleInterpreter {

  protected val rule: MappingConformanceRule
  protected val log: Logger

  protected def outputColumnNames(): String = rule.allOutputColumns().mkString(", ")

  protected def getOutputsStructColumnName(df: DataFrame): String = df.schema.getClosestUniqueName("outputs")

  protected val mappings: Seq[Mapping] = rule.attributeMappings.map {
    case (mappingTableField, dataframeField) => Mapping(mappingTableField, dataframeField)
  }.toSeq


  def conform(df: DataFrame)
             (implicit spark: SparkSession,
              explosionState: ExplosionState,
              dao: EnceladusDAO,
              progArgs: InterpreterContextArgs): DataFrame

  protected def conformPreparation(df: DataFrame, enableCrossJoin: Boolean)
                                  (implicit spark: SparkSession,
                                   dao: EnceladusDAO,
                                   progArgs: InterpreterContextArgs): (DataFrame, Map[String, String]) = {
    if (enableCrossJoin) {
      //A fix for cases, where the join condition only uses columns previously created by a literal rule
      //see https://github.com/AbsaOSS/enceladus/issues/892
      spark.conf.set("spark.sql.crossJoin.enabled", "true")
    }

    val mappingTableDef = dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)

    val ruleFilter = rule.mappingTableFilter
    val mappingTableFilter = mappingTableDef.filter.filterNot(_ => rule.getOverrideMappingTableOwnFilter)

    // find the data frame from the mapping table
    val filter: Option[DataFrameFilter] = (ruleFilter, mappingTableFilter) match {
      case (Some(a), Some(b)) => Option(a and b)
      case (Some(a), None)    => Option(a)
      case (None, Some(a))    => Option(a)
      case (None, None)       => None
    }
    val mapTable = DataSource.getDataFrame(mappingTableDef.hdfsPath, progArgs.reportDate, filter)

    if (mapTable.head(1).isEmpty) {
      log.warn(s"Mapping table ${mappingTableDef.name} is empty")
    }

    // join & perform projection on the target attribute
    val joinConditionStr = joinCondition.toString
    log.info("Mapping table: \n" + mapTable.schema.treeString)
    log.info("Rule: " + this.toString)
    log.info("Join Condition: " + joinConditionStr)

    // validate the default value against the mapping table schema
    val defaultValues: Map[String, String] = getDefaultValues(mappingTableDef)

    // validate join fields existence
    validateMappingFieldsExist(joinConditionStr, df.schema, mapTable.schema, rule)

    validateOutputColumns(rule)

    (mapTable, defaultValues)
  }

  protected def joinDatasetAndMappingTable(mapTable: DataFrame, dfIn: Dataset[Row]): DataFrame = {
    dfIn.as(CommonMappingRuleInterpreter.inputDfAlias).
      join(mapTable.as(CommonMappingRuleInterpreter.mappingTableAlias), joinCondition, CommonMappingRuleInterpreter.joinType).
      select(
        col(s"${CommonMappingRuleInterpreter.inputDfAlias}.*"),
        col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.${rule.targetAttribute}") as rule.outputColumn
      )
  }

  protected def validateMappingFieldsExist(joinConditionStr: String,
                                           datasetSchema: StructType,
                                           mappingTableSchema: StructType,
                                           rule: MappingConformanceRule): Unit = {
    // nothing to do here in general
  }

  protected def joinCondition: Column = CommonMappingRuleInterpreter.getJoinCondition(rule)

  /**
   * Returns a map of the default values of the output columns, if specified, for a particular mapping rule.
   * Default values may be specified for each target attribute in a mapping table and must have the same type as
   * the target attribute and must be presented as a Spark expression string.
   *
   * When a mapping table definition has a default value for the target attribute "*", this value acts as a default
   * value for all target attributes, for which the default value is not set.
   *
   * A target attribute used is specified in a mapping rule definition in the list of conformance rules in the dataset.
   *
   * @param mappingTableDef A mapping rule definition
   * @return A map of defaults as strings of Spark expressions, where the requested fields are the map's keys
   */
  private def getDefaultValues(mappingTableDef: MappingTable)
                              (implicit spark: SparkSession, dao: EnceladusDAO): Map[String, String] = {
    val defaultMappingValueMap = mappingTableDef.getDefaultMappingValueAsMap

    val genericDefaultValueOpt = defaultMappingValueMap.get("*")
    val defaultValuesForTargets = rule.allOutputColumns().flatMap {case (_, targetAttribute) =>
      val fieldDefault = defaultMappingValueMap.get(targetAttribute)
      fieldDefault match {
        case None => genericDefaultValueOpt.map(genDefault => targetAttribute -> genDefault)
        case Some(x) => Some(targetAttribute -> x)
      }
    }

    val mappingTableSchemaOpt = Option(dao.getSchema(mappingTableDef.schemaName, mappingTableDef.schemaVersion))
    mappingTableSchemaOpt match {
      case Some(schema) =>
        if(defaultValuesForTargets.nonEmpty){
          defaultValuesForTargets.foreach { case (targetAttribute, defaultValue: String) =>
            CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema(mappingTableDef.name, schema,
              targetAttribute, defaultValue)
          }
        }
      case None =>
        log.warn("Mapping table schema loading failed")
    }
    defaultValuesForTargets
  }

  private def validateOutputColumns(rule: MappingConformanceRule): Unit = {
    val outputColParent = SchemaUtils.getParentPath(rule.outputColumn)
    val allOutputsOnTheSamePath = rule.allOutputColumns().keys.map(SchemaUtils.getParentPath).forall(_ == outputColParent)
    if (! allOutputsOnTheSamePath) {
      throw new ValidationException(s"The output columns of a Mapping Conformance rule have to be on the same level")
    }
  }
}

object CommonMappingRuleInterpreter {
  val inputDfAlias = "input"
  val mappingTableAlias = "mapTable"
  val joinType = "left_outer"

  /**
    * Checks if a default value type can be used for a target attribute in Mapping rule.
    * Throws a MappingValidationException exception if there is a casting error.
    *
    * @param mappingTable The name of a mapping table. Used to construct an error message only
    * @param schema The schema of a mapping table
    * @param defaultValue A default value as a Spark expression
    * @param spark (implicit) A Spark Session
    *
    */
  @throws[ValidationException]
  def ensureDefaultValueMatchSchema(mappingTable: String,
                                    schema: StructType,
                                    targetAttribute: String,
                                    defaultValue: String)
                                   (implicit spark: SparkSession): Unit = {
    val targetField = getQualifiedField(schema, targetAttribute)

    if (targetField.isEmpty) {
      throw new ValidationException(
        s"The mapping table '$mappingTable' does not contain the specified target attribute '$targetAttribute'\n")
    }

    val targetAttributeType = targetField.get.dataType

    // Put all checks inside a Try object so we could intercept any exception and pack it
    // into a MappingValidationException.
    Try({
      if (defaultValue.trim.toLowerCase == "null") {
        require(targetField.get.nullable, "The target field is not nullable, 'null' is not acceptable.")
      } else {
        ExpressionValidator.ensureExpressionMatchesType(defaultValue, targetAttributeType)
      }
    }).recover({
      // Constructing a common form of exception messages based on the exception type
      // If it is IllegalArgumentException, include the details in the message itself.
      // Otherwise use the original exception message and put it into techDetails field
      // of MappingValidationException.
      val typeText = targetAttributeType.prettyJson
      val msg = s"The default value \n'$defaultValue'\n set for mapping table '$mappingTable' does not match the target attribute's data " +
        s"type\n'$typeText' \n"

      val recoverFunction: PartialFunction[Throwable, Unit] = {
        case e: IllegalArgumentException => throw new ValidationException(msg + "Details: " + e.getMessage, "")
        case NonFatal(e)                 => throw new ValidationException(msg, e.getMessage)
      }
      recoverFunction
    }).get // throw the exception of Try if it is a Failure
  }

  /**
    * getJoinCondition Function which builds a column object representing the join condition for mapping operation
    */
  def getJoinCondition(rule: MappingConformanceRule): Column = {
    def joinNullSafe(colNames: (String, String) ): Column = {
      col(s"$inputDfAlias.${colNames._2}") <=> col(s"$mappingTableAlias.${colNames._1}")
    }
    def joinPlain(colNames: (String, String) ): Column = {
      col(s"$inputDfAlias.${colNames._2}") === col(s"$mappingTableAlias.${colNames._1}")
    }

    val pairs = rule.attributeMappings.toList

    if (pairs.nonEmpty) {
      val joinFnc: ((String, String)) => Column = if (rule.isNullSafe) {
        joinNullSafe
      } else {
        joinPlain
      }
      val cond = pairs.tail.foldLeft(joinFnc(pairs.head))({
        case (acc: Column, attrs: (String, String)) => acc and joinFnc(attrs)
      })
      cond
    } else {
      lit(true)
    }
  }

  private def getQualifiedField(schema: StructType, fieldName: String): Option[StructField] = {
    val flatSchema = flattenForJoin(schema)
    flatSchema.find(_.name == fieldName)
  }

  // Flattens a schema for join validation purposes.
  private def flattenForJoin(schema: StructType, prefix: Option[String] = None): Seq[StructField] = {
    schema.fields.flatMap(field => {
      val fieldName = prefix.getOrElse("") + field.name
      val fld = field.copy(name = fieldName)
      field.dataType match {
        case s: StructType =>
          Seq(fld) ++ flattenForJoin(s, Some(fieldName + "."))
        case _ =>
          Seq(fld)
      }
    })
  }
}
