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

package za.co.absa.enceladus.conformance.interpreter

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import za.co.absa.atum.AtumImplicits._
import za.co.absa.enceladus.conformance.config.ConformanceConfigParser
import za.co.absa.enceladus.conformance.datasource.PartitioningUtils
import za.co.absa.enceladus.conformance.interpreter.rules._
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.conformance.interpreter.rules.mapping.{MappingRuleInterpreter, MappingRuleInterpreterBroadcast, MappingRuleInterpreterGroupExplode}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.explode.ExplosionContext
import za.co.absa.enceladus.utils.fs.HadoopFsUtils
import za.co.absa.enceladus.utils.general.Algorithms
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.udf.UDFLibrary

case class DynamicInterpreter(implicit inputFs: FileSystem) {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Interpret conformance rules defined in a dataset.
    *
    * @param conformance  The dataset object - this represents a data conformance workflow.
    * @param inputDf      The dataset to be conformed.
    * @param jobShortName A job name used for checkpoints.
    * @return The conformed DataFrame.
    *
    */
  def interpret[T](conformance: ConfDataset, inputDf: Dataset[Row], jobShortName: String = "Conformance")
               (implicit spark: SparkSession,
                dao: MenasDAO,
                progArgs: ConformanceConfigParser[T],
                featureSwitches: FeatureSwitches): DataFrame = {

    implicit val interpreterContext: InterpreterContext = InterpreterContext(inputDf.schema, conformance,
      featureSwitches, jobShortName, spark, dao, InterpreterContextArgs.fromConformanceConfig(progArgs))

    applyCheckpoint(inputDf, "Start")

    val conformedDf = applyConformanceRules(ensureErrorColumnExists(inputDf))

    applyCheckpoint(conformedDf, "End")
    logExecutionPlan(conformedDf)

    conformedDf
  }

  private def findOriginalColumnsModificationRules(steps: List[ConformanceRule],
                                                   schema: StructType): Seq[ConformanceRule] = {
    steps.filter(rule => SchemaUtils.fieldExists(rule.outputColumn, schema))
  }

  /**
    * Applies conformance rules applying a workaround for the Catalyst optimizer bug.
    *
    * @param inputDf The dataset to be conformed.
    * @return The conformed DataFrame.
    */
  private def applyConformanceRules(inputDf: DataFrame)
                                   (implicit ictx: InterpreterContext): DataFrame = {
    implicit val spark: SparkSession = ictx.spark
    implicit val dao: MenasDAO = ictx.dao
    implicit val progArgs: InterpreterContextArgs = ictx.progArgs
    implicit val udfLib: UDFLibrary = new UDFLibrary
    implicit val explosionState: ExplosionState = new ExplosionState()

    val steps = getConformanceSteps

    checkMutabilityNotViolated(inputDf.schema, steps)

    val interpreters = getInterpreters(steps, inputDf.schema)
    val optimizerTimeTracker = new OptimizerTimeTracker(inputDf, ictx.featureSwitches.catalystWorkaroundEnabled)
    val dfInputWithIdForWorkaround = optimizerTimeTracker.getWorkaroundDataframe

    // Fold left on rules
    var rulesApplied = 0
    val conformedDf = interpreters.foldLeft(dfInputWithIdForWorkaround)({
      case (df, interpreter) =>
        val explosionStateCopy = new ExplosionState(explosionState.explodeContext)
        val ruleAppliedDf = interpreter.conform(df)(spark, explosionStateCopy, dao, progArgs)

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
          case None       => conformedDf
        }
    })
    optimizerTimeTracker.cleanupWorkaroundDf(conformedDf)
  }

  private def checkMutabilityNotViolated(schema: StructType, steps: List[ConformanceRule])
                                        (implicit ictx: InterpreterContext): Unit = {
    val rulesInViolation = findOriginalColumnsModificationRules(steps, schema)

    if (rulesInViolation.nonEmpty) {
      val violationsString = rulesInViolation.map(rule =>
        s"Rule number ${rule.order} - ${rule.getClass.getSimpleName}"
      ).mkString("\n")
      if (ictx.featureSwitches.allowOriginalColumnsMutability) {
        log.warn(
          s"""Mutability of original Data Allowed and there are some rules in violation of immutability pattern.
             |These are:
             |$violationsString""".stripMargin)
      } else {
        throw new IllegalStateException(
          s"""There are some rules in violation of immutability pattern. These are:
             |$violationsString""".stripMargin)
      }
    }
  }

  /**
    * Transforms a list of conformance rules to a list of conformance rule interpreters.
    * For most conformance rules there is only one interpreter to apply. But mapping rule
    * has several strategies. Optimizer chooses which strategy to use and provides an
    * interpreter for each strategy.
    *
    * @param rules  A list of conformance rules.
    * @param schema A schema of a DataFrame to be conformed.
    * @return A list of conformance rule interpreters.
    */
  def getInterpreters(rules: List[ConformanceRule], schema: StructType)
                     (implicit ictx: InterpreterContext): List[RuleInterpreter] = {

    val groupedRules = groupMappingRules(rules, schema)

    getOptimizedInterpreters(groupedRules, schema)
  }

  /**
    * Optimizes a list of groups of conformance rules and returns a list of interpreters as the result.
    *
    * The input conformance rules are expected to be grouped. Only mapping rules are grouped. Each group of
    * mapping rules has the output field in the same array. This makes it possible to apply an optimization of
    * exploding the array only once. The optimization is done by inserting `ArrayExplodeInterpreter` and
    * `ArrayCollapseInterpreter` for each group of mapping rules.
    *
    * @param ruleGroups Conformance rules grouped by output field being in the same array.
    * @param schema     A schema of a DataFrame to be conformed.
    * @return A list of conformance rules interpreters.
    */
  private def getOptimizedInterpreters(ruleGroups: List[List[ConformanceRule]],
                                       schema: StructType)
                                      (implicit ictx: InterpreterContext): List[RuleInterpreter] = {
    ruleGroups.flatMap(rules => {
      val interpreters = rules.map(rule => getInterpreter(rule))
      if (isGroupExplosionUsable(rules) &&
        ictx.featureSwitches.experimentalMappingRuleEnabled) {
        // Inserting an explosion and a collapse between a group of mapping rules operating on a common array
        val optArray = SchemaUtils.getDeepestArrayPath(schema, rules.head.outputColumn)
        optArray match {
          case Some(arrayColumn) =>
            new ArrayExplodeInterpreter(arrayColumn) :: (interpreters :+ new ArrayCollapseInterpreter())
          case None              =>
            throw new IllegalStateException("Unable to find a common array between fields: " +
              rules.map(_.outputColumn).mkString(", "))
        }
      } else {
        interpreters
      }
    })
  }

  /**
    * Returns an interpreter for a conformance rule. Most conformance rules correspond to one interpreter.
    * The exception is the mapping rule for which there are several interpreters based on the strategy used.
    *
    * @param rule   A conformance rule.
    * @return A conformance rules interpreter.
    */
  private def getInterpreter(rule: ConformanceRule)
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
      case r: MappingConformanceRule          => getMappingRuleInterpreter(r)
      case r: FillNullsConformanceRule        => FillNullsRuleInterpreter(r)
      case r: CoalesceConformanceRule         => CoalesceRuleInterpreter(r)
      case r: CustomConformanceRule           => r.getInterpreter()
      case r                                  => throw new IllegalStateException(s"Unrecognized rule class: ${r.getClass.getName}")
    }
  }

  /**
    * Returns an interpreter for a mapping rule based on which strategy is applicable.
    *
    * @param rule   A conformance rule.
    * @return A mapping rule interpreter.
    */
  private def getMappingRuleInterpreter(rule: MappingConformanceRule)
                                       (implicit ictx: InterpreterContext): RuleInterpreter = {
    if (canMappingRuleBroadcast(rule)) {
      log.info("Broadcast strategy for mapping rules is used")
      MappingRuleInterpreterBroadcast(rule, ictx.conformance)
    } else {
      if (ictx.featureSwitches.experimentalMappingRuleEnabled) {
        log.info("Group explode strategy for mapping rules used")
        MappingRuleInterpreterGroupExplode(rule, ictx.conformance)
      } else {
        log.info("Explode strategy for mapping rules used")
        MappingRuleInterpreter(rule, ictx.conformance)
      }
    }
  }

  /**
    * An explosion is needed for a group of mapping rules if the number of mapping rules inside the group
    * for which broadcasting strategy is not applicable is bigger than 1.
    *
    * @param rules  A list of conformance rules grouped by output field being in the same array
    * @return true if a group explosion optimization can be used
    */
  private def isGroupExplosionUsable(rules: List[ConformanceRule])
                                    (implicit ictx: InterpreterContext): Boolean = {
    val eligibleRulesCount = rules.map {
      case rule: MappingConformanceRule => if (canMappingRuleBroadcast(rule)) 0 else 1
      case _                            => 0
    }.sum

    eligibleRulesCount > 1
  }

  /**
    * Returns true if broadcasting strategy is applicable for the specified mapping rule.
    *
    * @param rule   A mapping conformance rule.
    * @return true if the broadcasting mapping rule strategy can be used.
    */
  private def canMappingRuleBroadcast(rule: MappingConformanceRule)
                                     (implicit ictx: InterpreterContext): Boolean = {
    ictx.featureSwitches.broadcastStrategyMode match {
      case Always => true
      case Never => false
      case Auto => isMappingTableSmallEnough(rule)
    }
  }

  /**
    * Returns true if the mapping table size is small enough for the broadcasting strategy to be used.
    *
    * @param rule   A mapping conformance rule.
    * @return true if the mapping table size is small enough.
    */
  private def isMappingTableSmallEnough(rule: MappingConformanceRule)
                                       (implicit ictx: InterpreterContext): Boolean = {
    val maxBroadcastSizeMb = ictx.featureSwitches.broadcastMaxSizeMb
    val mappingTableSize = getMappingTableSizeMb(rule)
    log.info(s"Mapping table (${rule.mappingTable}) size = $mappingTableSize MB (threshold = $maxBroadcastSizeMb MB)")
    mappingTableSize <= maxBroadcastSizeMb
  }

  /**
    * Returns the size of the mapping table in megabytes.
    *
    * @param rule   A mapping conformance rule.
    * @return The size of the mapping table in megabytes.
    */
  private def getMappingTableSizeMb(rule: MappingConformanceRule)
                                   (implicit ictx: InterpreterContext): Int = {

    val mappingTableDef = ictx.dao.getMappingTable(rule.mappingTable, rule.mappingTableVersion)
    val mappingTablePath = PartitioningUtils.getPartitionedPathName(mappingTableDef.hdfsPath,
      ictx.progArgs.reportDate)
    val mappingTableSize = HadoopFsUtils.getOrCreate(inputFs).getDirectorySizeNoHidden(mappingTablePath)
    (mappingTableSize / (1024 * 1024)).toInt
  }

  /**
    * Gets the list of conformance rules from the context
    *
    * @return A list of conformance rules
    */
  def getConformanceSteps(implicit ictx: InterpreterContext): List[ConformanceRule] = {
    ictx.conformance.conformance.sortBy(_.order)
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
        case None        => df.cache
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
    // Need to check this explicitly since the execution plan generation can take significant amount of time
    if (log.isDebugEnabled) {
      val explain = ExplainCommand(df.queryExecution.logical, extended = true)
      spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
        r => log.debug("Output Dataset plan: \n" + r.getString(0))
      }
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
      case _                         => None
    }).map(_.toList).toList
  }

}
