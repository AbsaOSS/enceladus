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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import za.co.absa.enceladus.utils.schema.SchemaUtils

class OptimizerTimeTracker(inputDf: DataFrame) {
  private val log = LogManager.getLogger(this.getClass)

  private val maxToleratedPlanGenerationPerRuleMs = 100L
  private val initialElapsedTimeBaselineMs = 300L
  private var baselineTimeMs = initialElapsedTimeBaselineMs

  private val idField = SchemaUtils.getUniqueName("tmpId", Option(inputDf.schema))
  private val dfWithId = inputDf.withColumn(idField, monotonically_increasing_id)
  private val dfJustId = dfWithId.select(col(idField)).cache()

  /**
    * Returns a dataframe prepared to apply the Catalyst workaround
    */
  def getWorkaroundDataframe: DataFrame = dfWithId

  /**
    * Cleans up the additional field used for applying the Catalyst workaround
    */
  def cleanupWorkaroundDf(df: DataFrame): DataFrame = df.drop(col(idField))

  /**
    * Returns true of a dataframe might require a Catalyst issue workaround.
    * A workaround is required if execution plan optimization step takes too long
    *
    * @param df A dataframe that might require a Catalyst workaround
    * @return true if a workaround is required
    */
  def isCatalystWorkaroundRequired(df: DataFrame, rulesApplied: Int): Boolean = {
    val elapsedTime = getExecutionPlanGenerationTimeMs(df)
    val tooLong = elapsedTime > baselineTimeMs + maxToleratedPlanGenerationPerRuleMs * rulesApplied
    val msg = s"Execution optimization time for $rulesApplied rules: $elapsedTime ms"
    if (tooLong) {
      log.warn(s"$msg (Too long!)")
    } else {
      baselineTimeMs = Math.max(baselineTimeMs, elapsedTime)
      log.info(s"New baseline execution plan optimization time: $baselineTimeMs ms")
      log.warn(msg)
    }
    tooLong
  }

  /**
    * Returns elapsed time of execution plan optimization
    *
    * @param df A dataframe to calculate execution plan optimization time
    * @return Elapsed time in milliseconds
    */
  def getExecutionPlanGenerationTimeMs(df: DataFrame): Long = {
    val t0 = System.currentTimeMillis()
    df.queryExecution.toString()
    val t1 = System.currentTimeMillis()
    t1 - t0
  }

  /**
    * Applies a Catalyst workaround by joining a dataframe with a dataframe containing only unique ids.
    *
    * @param dfWithId A dataframe containing a unique id field for which execution plan generation takes too long
    * @return A new dataframe with Catalyst optimizer issue workaround applied
    */
  def applyCatalystWorkAround(dfWithId: DataFrame): DataFrame = {
    log.warn("A Catalyst optimizer issue workaround is applied.")
    dfWithId.join(dfJustId, idField)
  }

}
