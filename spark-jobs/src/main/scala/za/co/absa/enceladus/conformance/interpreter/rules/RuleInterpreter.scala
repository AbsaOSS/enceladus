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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.conformance.config.ConformanceConfigInstance
import za.co.absa.enceladus.conformance.interpreter.ExplosionState
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.utils.transformations.ArrayTransformations

import scala.util.Try

trait RuleInterpreter {


  /**
    * Returns the conformance rule the interpreter is intended to interpret.
    * The return value is optional since some interpreters are generated during conformance rules processing optimization
    * and such intermediate interpreters are not associated with any conformance rule.
    *
    * @return A conformance rule
    */
  def conformanceRule: Option[ConformanceRule]

  /**
    * Conforms a DataFrame according to a conformance rule.
    *
    * @param df An input DataFrame
    * @return A conformed DataFrame
    */
  def conform(df: Dataset[Row])
             (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceConfigInstance): Dataset[Row]

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * inferStrictestType Function which takes a string value, tries to infer the strictest applicable SQL type and returns a column literal object
   *
   * @param input The string representing the literal value
   */
  def inferStrictestType(input: String): Column = {
    // TODO: use commons for determining the types - faster & more efficient than throwing & catching exceptions
    val intTry = Try({
      val parsed = input.toInt
      assert(parsed.toString == input)
      lit(parsed)
    })
    val longTry = Try({
      val parsed = input.toLong
      assert(parsed.toString == input)
      lit(parsed)
    })
    val doubleTry = Try({
      val parsed = input.toDouble
      assert(parsed.toString == input)
      lit(parsed)
    })
    val boolTry = Try(lit(input.toBoolean))

    (intTry orElse longTry orElse doubleTry orElse boolTry) getOrElse lit(input)
  }

  /**
   * Helper function to handle arrays. If there's an array within the path of targetColumn, this helper will apply arrayTransform. When flat, it will apply the specified fn.
   *
   *  @param targetColumn The column which is to be conformed and needs to be transformed
   *  @param df The original (intermediate) dataset
   *  @param fn The transformer taking flattened dataset and returning a transformed dataset
   *  @return Dataset, which has been transformed, supporting nested arrays and preserving the original array elements order
   */
  def handleArrays(targetColumn: String, df: Dataset[Row])(fn: Dataset[Row] => Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = ArrayTransformations.handleArrays(targetColumn, df)(fn)
}
