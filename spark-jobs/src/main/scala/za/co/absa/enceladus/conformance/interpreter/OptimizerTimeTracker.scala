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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import za.co.absa.enceladus.utils.schema.SchemaUtils

class OptimizerTimeTracker(inputDf: DataFrame, isWorkaroundEnabled: Boolean)(implicit spark: SparkSession) {
  import spark.implicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val maxToleratedPlanGenerationPerRuleMs = 100L
  private val initialElapsedTimeBaselineMs = 300L
  private var baselineTimeMs = initialElapsedTimeBaselineMs
  private var lastExecutionPlanOptimizationTime = 0L

  private val idField1 = SchemaUtils.getUniqueName("tmpId", Option(inputDf.schema))
  private val idField2 = s"${idField1}_2"
  private val dfWithId = inputDf.withColumn(idField1, lit(1))
  private val dfJustId = Seq(1).toDF(idField2).cache()

  /**
    * Returns a dataframe prepared to apply the Catalyst workaround
    */
  def getWorkaroundDataframe: DataFrame = {
    if (isWorkaroundEnabled) {
      dfWithId
    } else {
      inputDf
    }
  }

  /**
    * Cleans up the additional field used for applying the Catalyst workaround
    */
  def cleanupWorkaroundDf(df: DataFrame): DataFrame = df.drop(col(idField1))

  /**
    * Returns true of a dataframe might require a Catalyst issue workaround.
    * A workaround is required if execution plan optimization step takes too long
    *
    * @param df A dataframe that might require a Catalyst workaround
    * @return true if a workaround is required
    */
  def isCatalystWorkaroundRequired(df: DataFrame, rulesApplied: Int): Boolean = {
    if (isWorkaroundEnabled) {
      val currentExecutionPlanOptimizationTime = getExecutionPlanGenerationTimeMs(df)

      // The algorithm for determining when to apply a workaround is based on 2 triggers.
      // 1. If it takes 5 times longer to optimize an execution plan now in comparison to the execution plan
      //    optimization time  before the last conformance rules was applied, then it is too much.
      val tooBigTimeDifference = if (lastExecutionPlanOptimizationTime > 0) {
        currentExecutionPlanOptimizationTime / lastExecutionPlanOptimizationTime > 5
      } else {
        false
      }

      // 2. If the time it takes to optimize an execution plan grows faster than a linear function of number of applied
      //    rules, it is also considered too long. Consider a case when after each application of a conformance rule
      //    it takes twice as much time. The n.1 condition won't be triggered and the execution time will
      //    grow exponentially. This is a preotection against such a case.
      val tooLong = tooBigTimeDifference ||
        currentExecutionPlanOptimizationTime > baselineTimeMs + maxToleratedPlanGenerationPerRuleMs * rulesApplied

      val msg = s"Execution optimization time for $rulesApplied rules: $currentExecutionPlanOptimizationTime ms"
      if (tooLong) {
        log.warn(s"$msg (Too long!)")
      } else {
        // The last 'approved' execution plan time generation is remembered as a baseline so that the condition n.2 is
        // not triggered based solely on a long execution plan generation time. It should be triggered on a big
        // increase of execution plan generation time instead.
        baselineTimeMs = Math.max(baselineTimeMs, currentExecutionPlanOptimizationTime)
        lastExecutionPlanOptimizationTime = currentExecutionPlanOptimizationTime
        log.info(s"New baseline execution plan optimization time: $baselineTimeMs ms")
        log.warn(msg)
      }
      tooLong
    } else {
      false
    }
  }

  /**
    * Records execution plan optimization time.
    * This is used after a workaround and a conformance rule are applied to a dataframe.
    *
    * @param df A dataframe for measuring execution plan optimization time
    */
  def recordExecutionPlanOptimizationTime(df: DataFrame): Unit = {
    val elapsedTime = getExecutionPlanGenerationTimeMs(df)
    baselineTimeMs = Math.max(baselineTimeMs, elapsedTime)
    lastExecutionPlanOptimizationTime = elapsedTime
  }

  /**
    * Returns elapsed time of execution plan optimization
    *
    * @param df A dataframe to calculate execution plan optimization time
    * @return Elapsed time in milliseconds
    */
  def getExecutionPlanGenerationTimeMs(df: DataFrame): Long = {
    val t0 = System.currentTimeMillis()
    getOptimizedPlanNoCache(df)
    val t1 = System.currentTimeMillis()
    t1 - t0
  }

  /**
   * Returns the optimized execution plan of a dataframe. Ensures the optimizer is invoked.
   * `df.queryExecution.optimized` can return a cached of an already calculated value.
   * This method ensures that the optimizer actually runs.
   *
   * @param df A dataframe to calculate execution plan optimization time
   * @return Elapsed time in milliseconds
   */
  private def getOptimizedPlanNoCache(df: DataFrame): LogicalPlan = {
    df.sparkSession.sessionState.optimizer.execute(df.queryExecution.analyzed)
  }

  /**
    * Applies a Catalyst workaround by joining a dataframe with the dataframe containing only unique ids.
    *
    * @param dfWithId A dataframe containing a unique id field for which execution plan generation takes too long
    * @return A new dataframe with Catalyst optimizer issue workaround applied
    */
  def applyCatalystWorkaround(dfWithId: DataFrame): DataFrame = {
    if (isWorkaroundEnabled) {
      log.warn("A Catalyst optimizer issue workaround is applied.")
      dfWithId.crossJoin(dfJustId).drop(col(idField2))
    } else {
      dfWithId
    }
  }
}
