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

package za.co.absa.enceladus.conformance.interpreter

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions._
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.conformance.interpreter.rules._
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.explode.{ExplodeTools, ExplosionContext}

object DynamicInterpreter {
  // scalastyle:off method.length

  private val log = LogManager.getLogger("enceladus.conformance.DynamicInterpreter")
  private var enableControlFramework = true

  /**
    * interpret The dynamic conformance interpreter function
    *
    * @param conformance  The dataset object - this represents a data conformance workflow
    * @param inputDf      The dataset to be conformed
    * @param jobShortName A job name used for checkpoints
    *
    */
  def interpret(conformance: ConfDataset,
                inputDf: Dataset[Row],
                jobShortName: String = "Conformance",
                experimentalMappingRule: Boolean = true)
               (implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig, enableCF: Boolean): DataFrame = {
    import spark.implicits._

    implicit val udfLib = new UDFLibrary

    enableControlFramework = enableCF
    if (enableControlFramework) inputDf.setCheckpoint(s"$jobShortName - Start")

    // conformance rules
    val steps = conformance.conformance.sortBy(_.order)

    // add the error column if it's missing
    val dfWithErrorColumn = if (inputDf.columns.contains(ErrorMessage.errorColumnName)) inputDf else inputDf.withColumn(ErrorMessage.errorColumnName, typedLit(List[ErrorMessage]()))

    // Exploding all mapping rule arrays
    val (explodeDf, explodeContext) = if (experimentalMappingRule) {
      log.info("Exploding all arrays in all mapping rules...")
      explodeAllMappingRuleArrays(dfWithErrorColumn, steps)
    } else {
      (dfWithErrorColumn, ExplosionContext())
    }

    // fold left on rules
    val ds = steps.foldLeft(explodeDf)({
      case (df, rule) =>

        val confd = rule match {
          case r: DropConformanceRule => DropRuleInterpreter(r).conform(df)
          case r: ConcatenationConformanceRule => ConcatenationRuleInterpreter(r).conform(df)
          case r: MappingConformanceRule =>
            if (experimentalMappingRule) {
              MappingRuleInterpreter(r, conformance).conform(df)
            } else {
              MappingRuleInterpreterOld(r, conformance).conform(df)
            }
          case r: LiteralConformanceRule => LiteralRuleInterpreter(r).conform(df)
          case r: SingleColumnConformanceRule => SingleColumnRuleInterpreter(r).conform(df)
          case r: SparkSessionConfConformanceRule => SparkSessionConfRuleInterpreter(r).conform(df)
          case r: UppercaseConformanceRule => UppercaseRuleInterpreter(r).conform(df)
          case r: CastingConformanceRule => CastingRuleInterpreter(r).conform(df)
          case r: NegationConformanceRule => NegationRuleInterpreter(r).conform(df)
          case r: CustomConformanceRule => r.getInterpreter.conform(df)
        }

        applyCheckpoint(rule, confd, jobShortName, explodeContext)
    })

    // Imploding all arrays back
    val implodeDf = ExplodeTools.revertAllExplosions(ds, explodeContext, Some(ErrorMessage.errorColumnName))

    if (enableControlFramework) {
      implodeDf.setCheckpoint(s"$jobShortName - End", persistInDatabase = false)
    }

    val explain = ExplainCommand(implodeDf.queryExecution.logical, extended = true)
    spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => log.debug("Output Dataset plan: \n" + r.getString(0))
    }

    implodeDf
  }

  /**
    * Explodes all arrays for mapping rules so the dataframe can be joined to mapping tables
    *
    * @param inputDf An input dataframe
    * @param steps   A list of conformance rules
    */
  private def explodeAllMappingRuleArrays(inputDf: DataFrame,
                                          steps: List[ConformanceRule]): (DataFrame, ExplosionContext) = {
    var explodeContext: ExplosionContext = ExplosionContext()
    var explodedDf = inputDf
    steps.foreach {
      case r: MappingConformanceRule =>
        val (df, context) = ExplodeTools.explodeAllArraysInPath(r.outputColumn, explodedDf, explodeContext)
        explodeContext = context
        explodedDf = df
      case _ => // Nothing to do
    }
    (explodedDf, explodeContext)
  }

  /**
    * applyCheckpoint Function which takes a generic rule and the dataframe and applies the control framework
    * checkpoint if configured
    *
    * @param rule The conformance rule
    * @param df   Dataframe to apply the checkpoint on
    */
  private[conformance] def applyCheckpoint(rule: ConformanceRule,
                                           df: Dataset[Row],
                                           jobShortName: String,
                                           explodeContext: ExplosionContext): Dataset[Row] = {
    if (enableControlFramework && rule.controlCheckpoint) {
      val explodeFilter = explodeContext.getControlFrameworkFilter
      // Cache the data first since Atum will execute an action for each control metric
      val cachedDf = df.cache
      cachedDf.filter(explodeFilter)
        .setCheckpoint(s"$jobShortName (${rule.order}) - ${rule.outputColumn}")
      cachedDf
    }
    else {
      df
    }
  }

}
