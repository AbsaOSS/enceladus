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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.conformance.interpreter.rules.MappingRuleInterpreterGroupExplode._
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error._
import za.co.absa.enceladus.utils.explode.{ExplodeTools, ExplosionContext}
import za.co.absa.enceladus.utils.transformations.ArrayTransformations.arrCol
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations
import za.co.absa.enceladus.utils.validation._

import scala.util.Try
import scala.util.control.NonFatal

case class MappingRuleInterpreterGroupExplode(rule: MappingConformanceRule,
                                              conformance: ConfDataset,
                                              explodeContext: ExplosionContext)
  extends RuleInterpreter {
  // scalastyle:off null

  private val conf = ConfigFactory.load()

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    log.info(s"Processing mapping rule (explode-optimized) to conform ${rule.outputColumn}...")
    val mapPartitioning = conf.getString("conformance.mappingtable.pattern")
    val mappingTableDef = dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)

    // find the data frame from the mapping table
    val mapTable = DataSource.getData(mappingTableDef.hdfsPath, progArgs.reportDate, mapPartitioning)
    val joinContidionStr = getJoinCondition(rule).toString
    val defaultMappingValueMap = mappingTableDef.getDefaultMappingValues

    logJoinCondition(mapTable.schema, joinContidionStr)
    validateMappingRule(df, dao, mappingTableDef, mapTable, joinContidionStr, defaultMappingValueMap)

    val (explodedDf, expCtx) = if (explodeContext.explosions.isEmpty) {
      ExplodeTools.explodeAllArraysInPath(rule.outputColumn, df)
    } else {
      (df, explodeContext)
    }

    val joined = explodedDf.as(MappingRuleInterpreterGroupExplode.inputDfAlias)
      .join(mapTable.as(MappingRuleInterpreterGroupExplode.mappingTableAlias),
        MappingRuleInterpreterGroupExplode.getJoinCondition(rule), "left_outer")
      .select(col(s"${MappingRuleInterpreterGroupExplode.inputDfAlias}.*"),
        col(s"${MappingRuleInterpreterGroupExplode.mappingTableAlias}.${rule.targetAttribute}") as rule.outputColumn)

    val mappings = rule.attributeMappings.map(x => Mapping(x._1, x._2)).toSeq
    val mappingErrUdfCall = callUDF("confMappingErr", lit(rule.outputColumn),
      array(rule.attributeMappings.values.toSeq.map(arrCol(_).cast(StringType)): _*),
      typedLit(mappings))

    val placedDf = ExplodeTools.nestedRenameReplace(joined, rule.outputColumn, rule.outputColumn)

    val defaultMappingValue = defaultMappingValueMap.get(rule.targetAttribute)

    val arrayErrorCondition = expCtx.getArrayErrorCondition(rule.outputColumn)
    val errorsDf = addErrorsToErrCol(placedDf, rule.attributeMappings.values.toSeq, rule.outputColumn,
      defaultMappingValue, mappingErrUdfCall, (srcCols, outCol) => {
        outCol.isNull.and(arrayErrorCondition)
      })

    val implodeDf = if (explodeContext.explosions.isEmpty) {
      ExplodeTools.revertAllExplosions(errorsDf, expCtx, Some(ErrorMessage.errorColumnName))
    } else {
      errorsDf
    }

    implodeDf
  }

  private def addErrorsToErrCol(df: DataFrame,
                                sourceCols: Seq[String],
                                outputCol: String,
                                defaultMappingValue: Option[String],
                                mappingErrUdfCall: Column,
                                errorCondition: (Seq[Column], Column) => Column): DataFrame = {

    val errorsDf = DeepArrayTransformations.nestedWithColumnAndErrorMap(df, outputCol, outputCol,
      ErrorMessage.errorColumnName,
      c => {
        defaultMappingValue match {
          case Some(defValue) => when(c.isNotNull, c).otherwise(expr(defValue))
          case None => c
        }
      }, _ => {
        when(errorCondition(sourceCols.map(col), col(outputCol)),
          mappingErrUdfCall).otherwise(null)
      }
    )
    errorsDf
  }

  private def validateMappingRule(df: Dataset[Row],
                                  dao: EnceladusDAO,
                                  mappingTableDef: MappingTable,
                                  mapTable: Dataset[Row],
                                  joinContidionStr: String,
                                  defaultMappingValueMap: Map[String, String])
                                 (implicit spark: SparkSession): Unit = {
    // validate the default value against the mapping table schema
    if (defaultMappingValueMap.contains(rule.targetAttribute)) {
      val mappingTableSchema = dao.getSchema(mappingTableDef.schemaName, mappingTableDef.schemaVersion)
      if (mappingTableSchema != null) {
        ensureDefaultValueMatchSchema(mappingTableDef.name,
          mappingTableSchema, rule.targetAttribute, defaultMappingValueMap(rule.targetAttribute))
      } else {
        log.warn("Mapping table schema loading failed")
      }
    }

    // validate join fields existence
    MappingRuleInterpreterGroupExplode.validateMappingFieldsExist(s"the dataset, join condition = $joinContidionStr",
      df.schema, mapTable.schema, rule)
  }

  private def logJoinCondition(mappingTableSchema: StructType, joinConditionStr: String): Unit = {
    log.info(s"Mapping table: \n${mappingTableSchema.treeString}")
    log.info(s"Rule: ${this.toString}")
    log.info(s"Join Condition: $joinConditionStr")
  }
}

object MappingRuleInterpreterGroupExplode {

  private[rules] val inputDfAlias = "input"
  private[rules] val mappingTableAlias = "mapTable"

  /**
    * Checks if a default value type can be used for a target attribute in Mapping rule.
    * Throws a MappingValidationException exception if there is a casting error.
    *
    * @param mappingTable The name of a mapping table. Used to construct an error message only
    * @param schema       The schema of a mapping table
    * @param defaultValue A default value as a Spark expression
    * @param spark        (implicit) A Spark Session
    *
    */
  @throws[ValidationException]
  private[rules] def ensureDefaultValueMatchSchema(mappingTable: String,
                                                   schema: StructType,
                                                   targetAttribute: String,
                                                   defaultValue: String)(implicit spark: SparkSession): Unit = {

    val targetField = getQualifiedField(schema, targetAttribute)

    if (targetField.isEmpty) {
      throw new ValidationException(
        s"The mapping table '$mappingTable' does not contain the specified target attribute '$targetAttribute'\n")
    }

    val targetAttributeType = targetField.get.dataType

    // Put all checks inside a Try object so we could intercept any exception and pack it
    // into a MappingValidationException.
    val validationResult = Try({
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
      val msg = s"The default value \n'$defaultValue'\n set for mapping table '$mappingTable' " +
        s"does not match the target attribute's data type\n'$typeText' \n"

      val recoverFunction: PartialFunction[Throwable, Any] = {
        case e: IllegalArgumentException => throw new ValidationException(msg + "Details: " + e.getMessage, "")
        case NonFatal(e) => throw new ValidationException(msg, e.getMessage)
      }
      recoverFunction
    })

    // Rethrow
    validationResult.get
  }

  @throws[ValidationException]
  private[rules] def validateMappingFieldsExist(datasetName: String,
                                                datasetSchema: StructType,
                                                mappingTableSchema: StructType,
                                                rule: MappingConformanceRule): Unit = {
    // Validate the output column
    validateOutputField(datasetName, datasetSchema, rule.outputColumn)

    // Validate the target attribute
    validateTargetAttribute(rule.mappingTable, mappingTableSchema, rule.targetAttribute)

    // Validate the join condition
    rule.attributeMappings.foreach {
      case (mappingTableField, datasetField) =>
        validateJoinField(rule.mappingTable, mappingTableSchema, mappingTableField)
        validateJoinField(datasetName, datasetSchema, datasetField)
    }
  }

  @throws[ValidationException]
  def validateJoinField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName,
      "A join condition validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  def validateTargetAttribute(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName,
      "A tagret attribute validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  def validateOutputField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPathParent(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName,
      "An output column name validation error occurred.", validationIssues)
  }

  private[rules] def getQualifiedField(schema: StructType, fieldName: String): Option[StructField] = {
    val flatSchema = flattenForJoin(schema)
    flatSchema.find(_.name == fieldName)
  }

  // Flattens a schema for join validation purposes.
  private[rules] def flattenForJoin(schema: StructType, prefix: Option[String] = None): Seq[StructField] = {
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

  /**
    * getJoinCondition Function which builds a column object representing the join condition for mapping operation
    *
    */
  private[rules] def getJoinCondition(rule: MappingConformanceRule): Column = {
    val pairs = rule.attributeMappings.toList

    def joinCond(c1: Column, c2: Column): Column = if (rule.isNullSafe) c1 <=> c2 else c1 === c2

    val cond = pairs.foldLeft(lit(true))({
      case (acc: Column, attrs: (String, String)) => acc and
        joinCond(col(s"$inputDfAlias.${attrs._2}"), col(s"$mappingTableAlias.${attrs._1}"))
    })
    cond
  }

}
