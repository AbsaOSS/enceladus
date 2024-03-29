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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.spark.commons.utils.ExplodeTools
import za.co.absa.spark.commons.utils.explode.ExplosionContext

/**
  * This conformance interpreter collapses previously exploded array(s) back.
  */
class ArrayCollapseInterpreter extends RuleInterpreter {

  override def conformanceRule: Option[ConformanceRule] = None

  override def conform(df: Dataset[Row])
                      (implicit spark: SparkSession, explosionState: ExplosionState, dao: EnceladusDAO,
                       progArgs: InterpreterContextArgs): Dataset[Row] = {
    val dfOut = ExplodeTools.revertAllExplosions(df, explosionState.explodeContext, Some(ErrorMessage.errorColumnName))
    explosionState.explodeContext = ExplosionContext()
    dfOut
  }
}
