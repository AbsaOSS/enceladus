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

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error._
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import za.co.absa.enceladus.utils.transformations.ArrayTransformations.arrCol
import za.co.absa.enceladus.utils.udf.UDFNames

case class MappingRuleInterpreter(rule: MappingConformanceRule, conformance: ConfDataset)
  extends RuleInterpreter with JoinMappingRuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  override def conform(df: DataFrame)
                      (implicit spark: SparkSession,
                       explosionState: ExplosionState,
                       dao: MenasDAO,
                       progArgs: InterpreterContextArgs): DataFrame = {
    log.info(s"Processing mapping rule to conform ${rule.outputColumn}...")

    //A fix for cases, where the join condition only uses columns previously created by a literal rule
    //see https://github.com/AbsaOSS/enceladus/issues/892
    val (mapTable, defaultValues) = conformPreparation(df, enableCrossJoin = true)
    val datasetSchema = dao.getSchema(conformance.schemaName, conformance.schemaVersion)
    val idField = rule.outputColumn.replace(".", "_") + "_arrayConformanceId"
    val withUniqueId = df.withColumn(idField, monotonically_increasing_id())
    var errorsDf = df

    val res = handleArrays(rule.outputColumn, withUniqueId) { dfIn =>
      val joined = joinDatasetAndMappingTable(mapTable, dfIn)
      val mappings = rule.attributeMappings.map(x => Mapping(x._1, x._2)).toSeq
      val mappingErrUdfCall = callUDF(UDFNames.confMappingErr, lit(rule.outputColumn),
        array(rule.attributeMappings.values.toSeq.map(arrCol(_).cast(StringType)): _*),
        typedLit(mappings))
      val appendErrUdfCall = callUDF(UDFNames.errorColumnAppend, col(ErrorMessage.errorColumnName), mappingErrUdfCall)
      errorsDf = joined.withColumn(
        ErrorMessage.errorColumnName,
        when(col(s"`${rule.outputColumn}`").isNull and inclErrorNullArr(mappings, datasetSchema), appendErrUdfCall).
          otherwise(col(ErrorMessage.errorColumnName)))

      val defaultValue = defaultValues.get(rule.targetAttribute)
      // see if we need to apply default value
      defaultValue match {
        case Some(defaultValue) =>
          ArrayTransformations
            .nestedWithColumn(joined)(rule.outputColumn, when(col(s"`${rule.outputColumn}`").isNotNull, col(s"`${rule.outputColumn}`"))
            .otherwise(expr(defaultValue)))
        case None =>
          ArrayTransformations.nestedWithColumn(joined)(rule.outputColumn, col(s"`${rule.outputColumn}`"))
      }
    }
    val errNested = errorsDf.groupBy(idField).agg(collect_list(col(ErrorMessage.errorColumnName)) as ErrorMessage.errorColumnName)
    val errNestedSchema = SchemaUtils.getFieldType(ErrorMessage.errorColumnName, errNested.schema).get.asInstanceOf[ArrayType]

    // errNested will duplicate error values if the previous rule has any errCol
    // and in the current rule the joining key is an array so the error values will duplicate as the size of array :
    // applied deduplicate logic while flattening error column
    spark.udf.register(s"${idField}_flattenErrDistinct",
      new UDF1[Seq[Seq[Row]], Seq[Row]] {
        override def call(t1: Seq[Seq[Row]]): Seq[Row] = {t1.flatten.distinct}
      },
      errNestedSchema.elementType)
    val withErr = errNested.withColumn(ErrorMessage.errorColumnName,
                                       expr(s"${idField}_flattenErrDistinct(${ErrorMessage.errorColumnName})"))
    import spark.implicits._
    // join on the errors
    res.drop(ErrorMessage.errorColumnName).as("conf")
      .join(withErr.as("err"), col(s"conf.$idField") === col(s"err.$idField"), "left_outer")
      .select($"conf.*", col(s"err.${ErrorMessage.errorColumnName}")).drop(idField)
  }

  private def inclErrorNullArr(mappings: Seq[Mapping], schema: StructType) = {
    val paths = mappings.flatMap { mapping =>
      SchemaUtils.getAllArraysInPath(mapping.mappedDatasetColumn, schema)
    }
    MappingRuleInterpreter.includeErrorsCondition(paths, schema)
  }
}

object MappingRuleInterpreter {
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
        case (acc: Column, (origPath, sizePath)) =>
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
