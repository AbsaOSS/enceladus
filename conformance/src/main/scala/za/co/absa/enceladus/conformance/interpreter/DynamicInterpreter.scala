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

import org.slf4j.LoggerFactory
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules._
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.explode.{ExplodeTools, ExplosionContext}
import za.co.absa.enceladus.utils.general.Algorithms
import za.co.absa.enceladus.utils.schema.SchemaUtils

object DynamicInterpreter {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * interpret The dynamic conformance interpreter function
    *
    * @param conformance                 The dataset object - this represents a data conformance workflow
    * @param inputDf                     The dataset to be conformed
    * @param jobShortName                A job name used for checkpoints
    * @return The conformed dataframe
    *
    */
  def interpret(conformance: ConfDataset, inputDf: Dataset[Row], jobShortName: String = "Conformance")
               (implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig, featureSwitches: FeatureSwitches): DataFrame = {

    implicit val interpreterContext: InterpreterContext = InterpreterContext(inputDf.schema, conformance,
      featureSwitches, jobShortName, spark, dao, progArgs)

    applyCheckpoint(inputDf, "Start")

    val conformedDf = applyConformanceRules(ensureErrorColumnExists(inputDf))

    applyCheckpoint(inputDf, "End")
    logExecutionPlan(conformedDf)

    conformedDf
  }

  /**
    * Applies conformance rules and corresponding optimizations and workarounds
    *
    * @param inputDf The dataset to be conformed
    * @return The conformed dataframe
    */
  private def applyConformanceRules(inputDf: DataFrame)
                                   (implicit ictx: InterpreterContext): DataFrame = {
    implicit val spark: SparkSession = ictx.spark
    implicit val dao: EnceladusDAO = ictx.dao
    implicit val progArgs: CmdConfig = ictx.progArgs
    implicit val udfLib: UDFLibrary = new UDFLibrary

    var explodeContext = ExplosionContext()

    val steps = getConformanceSteps
    val optimizerTimeTracker = new OptimizerTimeTracker(inputDf, ictx.featureSwitches.catalystWorkaroundEnabled)
    val dfInputWithIdForWorkaround = optimizerTimeTracker.getWorkaroundDataframe

    // Fold left on rules
    var rulesApplied = 0
    val conformedDf = steps.foldLeft(dfInputWithIdForWorkaround)({
      case (df, rule) =>
        val (ruleAppliedDf, ec) = applyConformanceRule(df, rule, explodeContext)
        explodeContext = ec

        val conformedDf = if (explodeContext.explosions.isEmpty &&
          optimizerTimeTracker.isCatalystWorkaroundRequired(ruleAppliedDf, rulesApplied)) {
          // Apply a workaround BEFORE applying the rule so that the execution plan generation still runs fast
          val (workAroundDf, ec) = applyConformanceRule(
            optimizerTimeTracker.applyCatalystWorkaround(df),
            rule,
            explodeContext)
          explodeContext = ec
          optimizerTimeTracker.recordExecutionPlanOptimizationTime(workAroundDf)
          workAroundDf
        } else {
          ruleAppliedDf
        }
        rulesApplied += 1
        applyRuleCheckpoint(rule, conformedDf, explodeContext)
    })
    optimizerTimeTracker.cleanupWorkaroundDf(conformedDf)
  }

  /**
    * Gets the list of conformance rules from the context and applies optimization rearrangements if needed
    *
    * @return A list of conformance rules
    */
  def getConformanceSteps(implicit ictx: InterpreterContext): List[ConformanceRule] = {
    val steps = ictx.conformance.conformance.sortBy(_.order)
    if (ictx.featureSwitches.experimentalMappingRuleEnabled) {
      getExplosionOptimizedSteps(steps, ictx.schema)
    } else {
      steps
    }
  }

  private def applyConformanceRule(df: DataFrame,
                                   rule: ConformanceRule,
                                   ec: ExplosionContext)
                                  (implicit ictx: InterpreterContext): (DataFrame, ExplosionContext) = {
    implicit val spark: SparkSession = ictx.spark
    implicit val dao: EnceladusDAO = ictx.dao
    implicit val progArgs: CmdConfig = ictx.progArgs

    var explodeContext = ec

    val confd = rule match {
      case r: DropConformanceRule             => DropRuleInterpreter(r).conform(df)
      case r: ConcatenationConformanceRule    => ConcatenationRuleInterpreter(r).conform(df)
      case r: LiteralConformanceRule          => LiteralRuleInterpreter(r).conform(df)
      case r: SingleColumnConformanceRule     => SingleColumnRuleInterpreter(r).conform(df)
      case r: SparkSessionConfConformanceRule => SparkSessionConfRuleInterpreter(r).conform(df)
      case r: UppercaseConformanceRule        => UppercaseRuleInterpreter(r).conform(df)
      case r: CastingConformanceRule          => CastingRuleInterpreter(r).conform(df)
      case r: NegationConformanceRule         => NegationRuleInterpreter(r).conform(df)
      case r: CustomConformanceRule           => r.getInterpreter().conform(df)
      case r: MappingConformanceRule          =>
        if (ictx.featureSwitches.experimentalMappingRuleEnabled) {
          MappingRuleInterpreterGroupExplode(r, ictx.conformance, explodeContext).conform(df)
        } else {
          MappingRuleInterpreter(r, ictx.conformance).conform(df)
        }
      // Array explode and collect pseudo rules apply array explosions for mapping rule groups that operate on
      // the same arrays
      case r: ArrayExplodePseudoRule          =>
        val (dfOut, ctx) = ExplodeTools.explodeAllArraysInPath(r.outputColumn, df, explodeContext)
        explodeContext = ctx
        dfOut
      case _: ArrayCollectPseudoRule          =>
        val dfOut = ExplodeTools.revertAllExplosions(df, explodeContext, Some(ErrorMessage.errorColumnName))
        explodeContext = ExplosionContext()
        dfOut
      case _ => throw new IllegalStateException(s"Unrecognized rule class: ${rule.getClass.getName}")
    }

    (confd, explodeContext)
  }

  /**
    * Transforms a list of steps (conformance rules) such as mapping rules operating on the same array will
    * grouped by a single explosion. The explode/collect pairs will be added between conformance rules
    * as corresponding pseudo-rules.
    *
    * @param inputSteps       The list of the rules to apply in the order of how they should be applied
    * @param schema The schema of the original dataframe to check which fields are arrays
    *
    * @return The transformed list of conformance rules
    */
  private[interpreter] def getExplosionOptimizedSteps(inputSteps: List[ConformanceRule],
                                                      schema: StructType): List[ConformanceRule] = {
    reorderConformanceRules(
      addExplosionsToMappingRuleGroups(
        groupMappingRules(inputSteps, schema), schema
      )
    )
  }

  /**
    * Applies a control framework checkpoint given a stage of the conformance
    *
    * @param df       Dataframe to apply the checkpoint on
    * @param jobStage Specifies a job stage that will be added to the checkpoint name
    */
  private def applyCheckpoint(df: Dataset[Row], jobStage: String)(implicit ictx: InterpreterContext): Unit = {
    if (ictx.featureSwitches.controlFrameworkEnabled) {
      df.setCheckpoint(s"${ictx.jobShortName} - $jobStage")
    }
  }

  /**
    * Create a new Control Framework checkpoint for a specified Conformance Rule (after the rule is applied)
    *
    * @param rule           The conformance rule
    * @param df             Dataframe to apply the checkpoint on
    * @param explodeContext An exploded context to be taken into account if a checkpoint is created for an exploded
    *                       dataframe
    * @return A cached dataframe if a checkpoint is calculated, otherwise returns the original dataframe
    */
  private def applyRuleCheckpoint(rule: ConformanceRule,
                                  df: Dataset[Row],
                                  explodeContext: ExplosionContext)
                                 (implicit ictx: InterpreterContext): Dataset[Row] = {
    if (ictx.featureSwitches.controlFrameworkEnabled && rule.controlCheckpoint) {
      val explodeFilter = explodeContext.getControlFrameworkFilter
      // Cache the data first since Atum will execute an action for each control metric
      val cachedDf = df.cache
      cachedDf.filter(explodeFilter)
        .setCheckpoint(s"${ictx.jobShortName} (${rule.order}) - ${rule.outputColumn}")
      cachedDf
    }
    else {
      df
    }
  }

  private def logExecutionPlan(df: DataFrame)(implicit spark: SparkSession): Unit = {
    val explain = ExplainCommand(df.queryExecution.logical, extended = true)
    spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => log.debug("Output Dataset plan: \n" + r.getString(0))
    }
  }

  /**
    * Ensures the existence of the error column
    *
    * @param inputDf the input data frame
    * @return A dataframe that has an error column
    */
  private def ensureErrorColumnExists(inputDf: Dataset[Row]): DataFrame = {
    if (inputDf.columns.contains(ErrorMessage.errorColumnName)) {
      inputDf
    } else {
      inputDf.withColumn(ErrorMessage.errorColumnName, typedLit(List[ErrorMessage]()))
    }
  }

  /**
    * Groups mapping rules if their output columns are inside the same array
    *
    * @param rules  a list of conformance rules
    * @param schema a schema of a dataset
    * @return The list of lists of conformance rule groups
    */
  private def groupMappingRules(rules: List[ConformanceRule], schema: StructType): List[List[ConformanceRule]] = {
    Algorithms.stableGroupByOption[ConformanceRule, String](rules, {
      case m: MappingConformanceRule => SchemaUtils.getDeepestArrayPath(schema, m.outputColumn)
      case _ => None
    }).map(_.toList).toList
  }

  /**
    * Adds an explosion pseudo-rule before each group of more than 1 mapping rule.
    * Adds a collection pseudo-rule after each group of more than 1 mapping rule
    *
    * @param ruleGroups A list of rules grouped together if they are mapping rules operating at the same array level
    * @param schema     A schema of a dataset
    * @return The new list of rules that might contain pseudo-rules
    */
  private def addExplosionsToMappingRuleGroups(ruleGroups: List[List[ConformanceRule]],
                                               schema: StructType): List[ConformanceRule] = {
    ruleGroups.flatMap(rules => {
      if (rules.lengthCompare(1) > 0) {
        val optArray = SchemaUtils.getDeepestArrayPath(schema, rules.head.outputColumn)
        optArray match {
          case Some(arr) =>
            (ArrayExplodePseudoRule(0, arr, controlCheckpoint = false) :: rules) :+
              ArrayCollectPseudoRule(0, "", controlCheckpoint = false)
          case None =>
            throw new IllegalStateException("")
        }
      } else {
        rules
      }
    })
  }

  /**
    * Reorders a list of conformance rules according to their appearance in the list.
    *
    * @param rules A list of rules to reorder
    * @return A reordered list of rules
    */
  private def reorderConformanceRules(rules: List[ConformanceRule]): List[ConformanceRule] = {
    var index = 0
    rules.map(rule => {
      index += 1
      rule.withUpdatedOrder(index)
    })
  }
}
