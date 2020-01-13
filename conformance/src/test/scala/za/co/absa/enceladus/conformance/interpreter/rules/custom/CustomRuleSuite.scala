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

package za.co.absa.enceladus.conformance.interpreter.rules.custom

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.mockito.Mockito.mock
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.cmd.ConformanceCmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, ExplosionState, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.model.{conformanceRule, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

case class MyCustomRule(
  order:             Int,
  outputColumn:      String,
  controlCheckpoint: Boolean, // this requires manual instantiation of control framework
  myCustomField:     String) extends CustomConformanceRule {
  def getInterpreter() = MyCustomRuleInterpreter(this)

  override def withUpdatedOrder(newOrder: Int): conformanceRule.ConformanceRule = copy(order = newOrder)
}

case class MyCustomRuleInterpreter(rule: MyCustomRule) extends RuleInterpreter {
  override def conformanceRule: Option[ConformanceRule] = Some(rule)

  def conform(df: Dataset[Row])(implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: ConformanceCmdConfig): Dataset[Row] = {
    import spark.implicits._
    // we have to do this if this rule is to support arrays
    handleArrays(rule.outputColumn, df) { flattened =>
      flattened.select(
        $"*", // preserve existing columns
        sqrt(col(rule.myCustomField)) as rule.outputColumn)
    }
  }
}

case class Mine(id: Int)
case class MineConfd(id: Int, myOutputCol: Double, errCol: Seq[ErrorMessage])

class CustomRuleSuite extends FunSuite with SparkTestBase {
  import spark.implicits._

  // we may WANT to enable control framework & spline here

  implicit val progArgs: ConformanceCmdConfig = ConformanceCmdConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: MenasDAO = mock(classOf[MenasDAO]) // you may have to hard-code your own implementation here (if not working with menas)
  val experimentalMR = true
  val isCatalystWorkaroundEnabled = true
  val enableCF: Boolean = false

  val inputData: DataFrame = spark.createDataFrame(Seq(Mine(1), Mine(4), Mine(9), Mine(16)))

  val conformanceDef = ConfDataset(
    name = "My dummy conformance workflow", // whatever here
    version = 0, //whatever here
    hdfsPath = "/a/b/c",
    hdfsPublishPath = "/publish/a/b/c",

    schemaName = "Not really used here",
    schemaVersion = 9999, //also not used

    conformance = List(
      MyCustomRule(order = 0, outputColumn = "myOutputCol", controlCheckpoint = false, myCustomField = "id")
    )
  )

  private val expected = Seq(MineConfd(1, 1d, Seq()), MineConfd(4, 2d, Seq()), MineConfd(9, 3d, Seq()), MineConfd(16, 4d, Seq()))
  implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
    .setExperimentalMappingRuleEnabled(experimentalMR)
    .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
    .setControlFrameworkEnabled(enableCF)

  val actualDf: DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)

  val actual: Seq[MineConfd] = actualDf.as[MineConfd].collect().toSeq

  test("Testing custom rule results") {
    assertResult(expected)(actual)
  }

}
