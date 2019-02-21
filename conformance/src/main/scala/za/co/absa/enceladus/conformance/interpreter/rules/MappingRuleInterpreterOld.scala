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

import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.transformations.ArrayTransformations.arrCol
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.utils.schema.SchemaUtils
import org.apache.spark.sql.api.java.UDF1
import za.co.absa.enceladus.utils.error._
import za.co.absa.enceladus.model.{Dataset => ConfDataset}

import scala.util.Try
import scala.util.control.NonFatal
import org.apache.spark.sql.Column
import za.co.absa.enceladus.conformance.interpreter.RuleValidators
import za.co.absa.enceladus.utils.validation._

case class MappingRuleInterpreterOld(rule: MappingConformanceRule, conformance: ConfDataset) extends RuleInterpreter {

  private val conf = ConfigFactory.load()

  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    log.info(s"Processing mapping rule to conform ${rule.outputColumn}...")
    import spark.implicits._

    val datasetSchema = dao.getSchema(conformance.schemaName, conformance.schemaVersion)

    val idField = rule.outputColumn.replace(".", "_") + "_arrayConformanceId"
    val withUniqueId = df.withColumn(idField, monotonically_increasing_id())

    val mapPartitioning = conf.getString("conformance.mappingtable.pattern")
    val mappingTableDef = dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)

    // find the data frame from the mapping table
    val mapTable = DataSource.getData(mappingTableDef.hdfsPath, progArgs.reportDate, mapPartitioning)

    // join & perform projection on the target attribute
    val joinContidionStr = MappingRuleInterpreterOld.getJoinCondition(rule).toString
    log.info("Mapping table: \n" + mapTable.schema.treeString)
    log.info("Rule: " + this.toString)
    log.info("Join Condition: " + joinContidionStr)

    // validate the default value against the mapping table schema
    val defaultMappingValueMap = mappingTableDef.getDefaultMappingValues
    if (defaultMappingValueMap.contains(rule.targetAttribute)) {
      val mappingTableSchema = dao.getSchema(mappingTableDef.schemaName, mappingTableDef.schemaVersion)
      if (mappingTableSchema != null) {
        MappingRuleInterpreterOld.ensureDefaultValueMatchSchema(mappingTableDef.name, mappingTableSchema, rule.targetAttribute, defaultMappingValueMap(rule.targetAttribute))
      } else {
        log.warn("Mapping table schema loading failed")
      }
    }

    // validate join fields existence
    MappingRuleInterpreterOld.validateMappingFieldsExist(s"the dataset, join condition = $joinContidionStr", df.schema, mapTable.schema, rule)

    var errorsDf = df

    val res = handleArrays(rule.outputColumn, withUniqueId) { dfIn =>

      val joined = dfIn.as(MappingRuleInterpreterOld.inputDfAlias).join(mapTable.as(MappingRuleInterpreterOld.mappingTableAlias), MappingRuleInterpreterOld.getJoinCondition(rule), "left_outer").
        select(col(s"${MappingRuleInterpreterOld.inputDfAlias}.*"), col(s"${MappingRuleInterpreterOld.mappingTableAlias}.${rule.targetAttribute}") as rule.outputColumn)

      val mappings = rule.attributeMappings.map(x => Mapping(x._1, x._2)).toSeq
      val mappingErrUdfCall = callUDF("confMappingErr", lit(rule.outputColumn),
        array(rule.attributeMappings.values.toSeq.map(arrCol(_).cast(StringType)): _*),
        typedLit(mappings))

      val appendErrUdfCall = callUDF("errorColumnAppend", col(ErrorMessage.errorColumnName), mappingErrUdfCall)

      errorsDf = joined.withColumn(
        ErrorMessage.errorColumnName,
        when((col(s"`${rule.outputColumn}`") isNull) and inclErrorNullArr(mappings, datasetSchema), appendErrUdfCall).otherwise(col(ErrorMessage.errorColumnName)))

      // see if we need to apply default value
      val resDf = if (defaultMappingValueMap.contains(rule.targetAttribute)) {
        ArrayTransformations.nestedWithColumn(joined)(rule.outputColumn, when(col(s"`${rule.outputColumn}`") isNotNull, col(s"`${rule.outputColumn}`")).
          otherwise(expr(defaultMappingValueMap(rule.targetAttribute))))
      } else ArrayTransformations.nestedWithColumn(joined)(rule.outputColumn, col(s"`${rule.outputColumn}`"))

      resDf
    }

    val errNested = errorsDf.groupBy(idField).agg(collect_list(col(ErrorMessage.errorColumnName)) as ErrorMessage.errorColumnName)
    val errNestedSchema = SchemaUtils.getFieldType(ErrorMessage.errorColumnName, errNested.schema).get.asInstanceOf[ArrayType]

    // errNested will duplicate error values if the previous rule has any errCol
    // and in the current rule the joining key is an array so the error values will duplicate as the size of array :
    // applied deduplicate logic while flattening error column
    spark.udf.register(s"${idField}_flattenErrDistinct", new UDF1[Seq[Seq[Row]], Seq[Row]] {
      override def call(t1: Seq[Seq[Row]]) = {
        t1.flatten.distinct
      }

    }, errNestedSchema.elementType)

    val withErr = errNested.withColumn(ErrorMessage.errorColumnName, expr(s"${idField}_flattenErrDistinct(${ErrorMessage.errorColumnName})"))

    // join on the errors
    val res2 = res.drop(ErrorMessage.errorColumnName).as("conf")
      .join(withErr.as("err"), col(s"conf.$idField") === col(s"err.$idField"), "left_outer").select($"conf.*", col(s"err.${ErrorMessage.errorColumnName}")).drop(idField)

    res2
  }

  private def inclErrorNullArr(mappings: Seq[Mapping], schema: StructType) = {
    val paths = mappings.flatMap { mapping =>
      SchemaUtils.getAllArraysInPath(mapping.mappedDatasetColumn, schema)
    }
    MappingRuleInterpreterOld.includeErrorsCondition(paths, schema)
  }

}

object MappingRuleInterpreterOld {

  private[rules] val inputDfAlias = "input"
  private[rules] val mappingTableAlias = "mapTable"

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
  private[rules] def ensureDefaultValueMatchSchema(mappingTable: String, schema: StructType, targetAttribute: String, defaultValue: String)(implicit spark: SparkSession): Unit = {

    val targetField = getQualifiedField(schema, targetAttribute)

    if (targetField.isEmpty)
      throw new ValidationException(
        s"The mapping table '$mappingTable' does not contain the specified target attribute '$targetAttribute'\n")

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
      val msg = s"The default value \n'$defaultValue'\n set for mapping table '$mappingTable' does not match the target attribute's data " +
        s"type\n'$typeText' \n"

      val recoverFunction: PartialFunction[Throwable, Any] = {
        case e: IllegalArgumentException => throw new ValidationException(msg + "Details: " + e.getMessage, "")
        case NonFatal(e)                 => throw new ValidationException(msg, e.getMessage)
      }
      recoverFunction
    })

    // Rethrow
    validationResult.get
  }

  @throws[ValidationException]
  private[rules] def validateMappingFieldsExist(datasetName: String, datasetSchema: StructType, mappingTableSchema: StructType,
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
    RuleValidators.checkAndThrowValidationErrors(datasetName, "A join condition validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  def validateTargetAttribute(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPath(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName, "A tagret attribute validation error occurred.", validationIssues)
  }

  @throws[ValidationException]
  def validateOutputField(datasetName: String, schema: StructType, fieldPath: String): Unit = {
    val validationIssues = SchemaPathValidator.validateSchemaPathParent(schema, fieldPath)
    RuleValidators.checkAndThrowValidationErrors(datasetName, "An output column name validation error occurred.", validationIssues)
  }

  private[rules] def getQualifiedField(schema: StructType, fieldName: String): Option[StructField] = {
    val flatSchema = flattenForJoin(schema)
    val field = flatSchema.find(_.name == fieldName)
    if (field.isDefined)
      Some(field.get)
    else
      None
  }

  // Flattens a schema for join validation purposes.
  private[rules] def flattenForJoin(schema: StructType, prefix: Option[String] = None): Seq[StructField] = {
    schema.fields.flatMap(field => {
      val fieldName = prefix.getOrElse("") + field.name
      field.dataType match {
        case s: StructType =>
          val fld = field.copy(name = fieldName)
          Seq(fld) ++ flattenForJoin(s, Some(fieldName + "."))
        case _ =>
          val fld = field.copy(name = fieldName)
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
        joinCond(col(s"${inputDfAlias}.${attrs._2}"), col(s"${mappingTableAlias}.${attrs._1}"))
    })
    cond
  }

  /**
   * includeErrorsCondition Function which builds a column object representing the where clause for error column population
   *
   * If there is an array on the path of either side of a join condition
   *  if the array is nullable & (null or empty) -> do not produce any errors
   *    otherwise produce errors as usual
   *  if the array is NOT nullable & null -> produce error (this should never happen though, will be more of a sanity check)
   *  if the array is NOT nullable & empty -> no error
   *
   */
  private[rules] def includeErrorsCondition(paths: Seq[String], schema: StructType) = {
    paths
      .map(x => (x, ArrayTransformations.arraySizeCols(x)))
      .foldLeft(lit(true)) {
        case (acc: Column, (origPath, sizePath)) => {
          val nullable = lit(SchemaUtils.getFieldNullability(origPath, schema).get)
          val nll = col(sizePath) === lit(-1)          
          val empty = col(sizePath) === lit(0)
          
          acc and (
              (!empty and !nll) or
              (!nullable and nll)
          )
        }
      }
  }

}
