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
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.conformance.interpreter.rules._
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.explode.{ExplodeTools, ExplosionContext}

object DynamicInterpreter {
  private val log = LogManager.getLogger("enceladus.conformance.DynamicInterpreter")

  /**
    * interpret The dynamic conformance interpreter function
    *
    * @param conformance             The dataset object - this represents a data conformance workflow
    * @param inputDf                 The dataset to be conformed
    * @param jobShortName            A job name used for checkpoints
    * @param experimentalMappingRule If true the new explode-optimized conformance mapping rule interpreter will be used
    * @param enableControlFramework  If true sets the checkpoints on the dataset upon conforming
    * @return The conformed dataframe
    *
    */
  def interpret(conformance: ConfDataset,
                inputDf: Dataset[Row],
                experimentalMappingRule: Boolean,
                enableControlFramework: Boolean,
                jobShortName: String = "Conformance"
               )(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): DataFrame = {

    //noinspection TypeAnnotation
    implicit val interpreterContext = InterpreterContext(conformance, experimentalMappingRule, enableControlFramework,
      jobShortName, spark, dao, progArgs)

    applyCheckpoint(inputDf, "Start")

    val conformedDf: DataFrame = applyConformanceRules(ensureErrorColumnExists(inputDf))

    applyCheckpoint(inputDf, "End")
    logExecutionPlan(conformedDf)

    conformedDf
  }

  private def applyConformanceRules(inputValidDf: DataFrame)
                                   (implicit ictx: InterpreterContext): DataFrame = {
    implicit val spark: SparkSession = ictx.spark
    implicit val dao: EnceladusDAO = ictx.dao
    implicit val progArgs: CmdConfig = ictx.progArgs
    implicit val udfLib: UDFLibrary = new UDFLibrary

    val explodeContext = ExplosionContext()

    val steps = getConformanceSteps(ictx)

    // Fold left on rules
    val conformedDf = steps.foldLeft(inputValidDf)({
      case (df, rule) =>
        val confd = rule match {
          case r: DropConformanceRule => DropRuleInterpreter(r).conform(df)
          case r: ConcatenationConformanceRule => ConcatenationRuleInterpreter(r).conform(df)
          case r: LiteralConformanceRule => LiteralRuleInterpreter(r).conform(df)
          case r: SingleColumnConformanceRule => SingleColumnRuleInterpreter(r).conform(df)
          case r: SparkSessionConfConformanceRule => SparkSessionConfRuleInterpreter(r).conform(df)
          case r: UppercaseConformanceRule => UppercaseRuleInterpreter(r).conform(df)
          case r: CastingConformanceRule => CastingRuleInterpreter(r).conform(df)
          case r: NegationConformanceRule => NegationRuleInterpreter(r).conform(df)
          case r: CustomConformanceRule => r.getInterpreter().conform(df)
          case r: MappingConformanceRule =>
            if (ictx.experimentalMappingRule) {
              MappingRuleInterpreterNoExplode(r, ictx.conformance, explodeContext).conform(df)
            } else {
              MappingRuleInterpreter(r, ictx.conformance).conform(df)
            }
          case _ => throw new IllegalStateException(s"Unrecognized rule class: ${rule.getClass.getName}")
        }
        applyRuleCheckpoint(rule, confd, ictx.jobShortName, explodeContext, ictx.enableControlFramework)
    })
    conformedDf
  }

  private def getConformanceSteps(ictx: InterpreterContext): List[ConformanceRule] = {
    val steps = ictx.conformance.conformance.sortBy(_.order)
    if (ictx.experimentalMappingRule) {
      getExplosionOptimizedSteps(steps)
    } else {
      steps
    }
  }

  private def getExplosionOptimizedSteps(inputSteps: List[ConformanceRule]): List[ConformanceRule] = {
    // ToDo: Add explosion optimization here

    inputSteps
  }

  /**
    * Explodes all arrays for mapping rules so the dataframe can be joined to mapping tables
    *
    * @param inputDf An input dataframe
    * @param steps   A list of conformance rules
    */
  private def explodeAllMappingRuleArrays(inputDf: DataFrame,
                                          steps: List[ConformanceRule]): (DataFrame, ExplosionContext) = {

    steps.foldLeft((inputDf, ExplosionContext())) {
      case ((df, context), rule) =>
        rule match {
          case r: MappingConformanceRule =>
            ExplodeTools.explodeAllArraysInPath(r.outputColumn, df, context)
          case _ =>
            (df, context)
        }
    }
  }

  /** Applies a control framework checkpoint given a stage of the conformance */
  private def applyCheckpoint(df: Dataset[Row], jobStage: String)(implicit ictx: InterpreterContext): Unit = {
    if (ictx.enableControlFramework) {
      df.setCheckpoint(s"${ictx.jobShortName} - $jobStage")
    }
  }

  /**
    * applyRuleCheckpoint Function which takes a generic rule and the dataframe and applies the control framework
    * checkpoint if configured
    *
    * @param rule The conformance rule
    * @param df   Dataframe to apply the checkpoint on
    */
  private def applyRuleCheckpoint(rule: ConformanceRule,
                              df: Dataset[Row],
                              jobShortName: String,
                              explodeContext: ExplosionContext,
                              enableCF: Boolean): Dataset[Row] = {
    if (enableCF && rule.controlCheckpoint) {
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

  private def logExecutionPlan(df: DataFrame)(implicit spark: SparkSession): Unit = {
    val explain = ExplainCommand(df.queryExecution.logical, extended = true)
    spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => log.debug("Output Dataset plan: \n" + r.getString(0))
    }
  }

  /**
    * Ensures the existence of the error error column
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

}
