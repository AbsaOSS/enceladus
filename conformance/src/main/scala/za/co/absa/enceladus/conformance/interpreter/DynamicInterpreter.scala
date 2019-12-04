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

import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules._
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.explode.ExplosionContext
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
               (implicit spark: SparkSession, dao: MenasDAO, progArgs: CmdConfig, featureSwitches: FeatureSwitches): DataFrame = {

    implicit val interpreterContext: InterpreterContext = InterpreterContext(inputDf.schema, conformance,
      featureSwitches, jobShortName, spark, dao, progArgs)

    applyCheckpoint(inputDf, "Start")

    val conformedDf = applyConformanceRules(ensureErrorColumnExists(inputDf))

    applyCheckpoint(conformedDf, "End")
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
    implicit val dao: MenasDAO = ictx.dao
    implicit val progArgs: CmdConfig = ictx.progArgs
    implicit val udfLib: UDFLibrary = new UDFLibrary
    implicit val explosionState: ExplosionState = new ExplosionState()

    val steps = getConformanceSteps
    val interpreters = getInterpreters(steps, inputDf.schema)
    val optimizerTimeTracker = new OptimizerTimeTracker(inputDf, ictx.featureSwitches.catalystWorkaroundEnabled)
    val dfInputWithIdForWorkaround = optimizerTimeTracker.getWorkaroundDataframe

    // Fold left on rules
    var rulesApplied = 0
    val conformedDf = interpreters.foldLeft(dfInputWithIdForWorkaround)({
      case (df, interpreter) =>
        val explosionStateCopy = new ExplosionState(explosionState.explodeContext)
        val ruleAppliedDf =  interpreter.conform(df)(spark, explosionStateCopy, dao, progArgs)

        val conformedDf = if (explosionState.isNoExplosionsApplied &&
          optimizerTimeTracker.isCatalystWorkaroundRequired(ruleAppliedDf, rulesApplied)) {
          // Apply a workaround BEFORE applying the rule so that the execution plan generation still runs fast
          val workAroundDf = interpreter.conform(optimizerTimeTracker.applyCatalystWorkaround(df))
          optimizerTimeTracker.recordExecutionPlanOptimizationTime(workAroundDf)
          workAroundDf
        } else {
          explosionState.explodeContext = explosionStateCopy.explodeContext
          ruleAppliedDf
        }
        rulesApplied += 1
        interpreter.conformanceRule match {
          case Some(rule) => applyRuleCheckpoint(rule, conformedDf, progArgs.persistStorageLevel, explosionState.explodeContext)
          case None => conformedDf
        }
    })
    optimizerTimeTracker.cleanupWorkaroundDf(conformedDf)
  }

  private def getInterpreters(rules: List[ConformanceRule], schema: StructType)
                             (implicit ictx: InterpreterContext): List[RuleInterpreter] = {

    val groupedRules = groupMappingRules(rules, schema)

    // Get unoptimized list of interpreters
    getOptimizedInterpreters(groupedRules, schema)
  }

  private def getOptimizedInterpreters(ruleGroups: List[List[ConformanceRule]],
                                       schema: StructType)
                                      (implicit ictx: InterpreterContext): List[RuleInterpreter] = {
    val explosionState: ExplosionState = new ExplosionState()

    ruleGroups.flatMap(rules => {
      val interpreters = rules.map(rule => getInterpreter(rule, schema, explosionState))
      if (isGroupExploisonCanBeUsed(rules, schema) &&
        ictx.featureSwitches.experimentalMappingRuleEnabled) {
        // Inserting an explosion and a collapse between a group of mapping rules operating on a common array
        val optArray = SchemaUtils.getDeepestArrayPath(schema, rules.head.outputColumn)
        optArray match {
          case Some(arrayColumn) =>
            new ArrayExplodeInterpreter(arrayColumn) ::
              (interpreters :+ new ArrayCollapseInterpreter())
          case None =>
            throw new IllegalStateException("Unexpectedly unable to find common array amound fields: " +
              rules.map(_.outputColumn).mkString(", "))
        }
      } else {
        interpreters
      }
    })
  }

  private def getInterpreter(rule: ConformanceRule,
                             schema: StructType,
                             explosionState: ExplosionState)
                            (implicit ictx: InterpreterContext): RuleInterpreter = {
    rule match {
      case r: DropConformanceRule             => DropRuleInterpreter(r)
      case r: ConcatenationConformanceRule    => ConcatenationRuleInterpreter(r)
      case r: LiteralConformanceRule          => LiteralRuleInterpreter(r)
      case r: SingleColumnConformanceRule     => SingleColumnRuleInterpreter(r)
      case r: SparkSessionConfConformanceRule => SparkSessionConfRuleInterpreter(r)
      case r: UppercaseConformanceRule        => UppercaseRuleInterpreter(r)
      case r: CastingConformanceRule          => CastingRuleInterpreter(r)
      case r: NegationConformanceRule         => NegationRuleInterpreter(r)
      case r: MappingConformanceRule          => getMappingRuleInterpreter(r, schema)
      case r: CustomConformanceRule           => r.getInterpreter()
      case r: ArrayExplodePseudoRule          => new ArrayExplodeInterpreter(r.outputColumn)
      case _: ArrayCollectPseudoRule          => new ArrayCollapseInterpreter()
      case r => throw new IllegalStateException(s"Unrecognized rule class: ${r.getClass.getName}")
    }
  }

  private def getMappingRuleInterpreter(rule: MappingConformanceRule,
                                        schema: StructType)
                                       (implicit ictx: InterpreterContext): RuleInterpreter = {
    if (ictx.featureSwitches.experimentalMappingRuleEnabled) {
      if (canMappingRuleBroadcast(rule, schema)) {
        MappingRuleInterpreterBroadcast(rule, ictx.conformance)
      } else{
        MappingRuleInterpreterGroupExplode(rule, ictx.conformance)
      }
    } else {
      MappingRuleInterpreter(rule, ictx.conformance)
    }
  }

  /**
    * An explosion is needed for a group of mapping rules if the number of mapping rules inside the group
    * for which broadcasting strategy is not applicable is bigger than 1.
    *
    * @param rules  A list of conformance rules grouped by output field being in the same array
    * @param schema A schema of a dataset
    * @return true if a group explosion optimization can be used
    */
  private def isGroupExploisonCanBeUsed(rules: List[ConformanceRule], schema: StructType): Boolean = {
    rules.map {
      case rule: MappingConformanceRule => if (canMappingRuleBroadcast(rule, schema)) 1 else 0
      case _ => 0
    }.sum > 1
  }

  /**
    * Returns true if broadcasting strategy is applicable for the specified mapping rule.
    *
    * @param rule   A mapping conformance rule
    * @param schema A schema of a dataset
    * @return true if a group explosion optimization can be used
    */
  private def canMappingRuleBroadcast(rule: MappingConformanceRule, schema: StructType): Boolean = {
    // ToDo Currently, the broadcasting strategy is turned off. The decision when to use it should
    //      be implemented as part of #1017.
    false
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
    * @param rule                The conformance rule
    * @param df                  Dataframe to apply the checkpoint on
    * @param persistStorageLevel A storage level for caching/persisting the df, if set.
    * @param explodeContext      An exploded context to be taken into account if a checkpoint is created for an exploded
    *                            dataframe
    * @return A cached dataframe if a checkpoint is calculated, otherwise returns the original dataframe
    */
  private def applyRuleCheckpoint(rule: ConformanceRule,
                                  df: Dataset[Row],
                                  persistStorageLevel: Option[StorageLevel],
                                  explodeContext: ExplosionContext)
                                 (implicit ictx: InterpreterContext): Dataset[Row] = {
    if (ictx.featureSwitches.controlFrameworkEnabled && rule.controlCheckpoint) {
      val explodeFilter = explodeContext.getControlFrameworkFilter
      // Cache the data first since Atum will execute an action for each control metric
      val cachedDf = persistStorageLevel match {
        case Some(level) => df.persist(level)
        case None => df.cache
      }
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
