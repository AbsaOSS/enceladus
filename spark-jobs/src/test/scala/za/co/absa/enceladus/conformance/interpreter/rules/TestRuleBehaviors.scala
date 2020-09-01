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

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.FunSuite
import org.slf4j.event.Level._
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}


trait TestRuleBehaviors  extends FunSuite with SparkTestBase with LoggerTestBase {

  def conformanceRuleShouldMatchExpected(inputDf: DataFrame,
                                         inputDataset: Dataset,
                                         expectedJSON: String,
                                         featureSwitchesOverride: Option[FeatureSwitches] = None) {
    implicit val dao: MenasDAO = mock(classOf[MenasDAO])
    implicit val progArgs: ConformanceConfig = ConformanceConfig(reportDate = "2017-11-01")
    val experimentalMR = true
    val isCatalystWorkaroundEnabled = true
    val enableCF: Boolean = false
    val originalColumnsMutability: Boolean = true

    mockWhen(dao.getDataset("Orders Conformance", 1)) thenReturn inputDataset
    mockWhen(dao.getDataset("Library Conformance", 1)) thenReturn inputDataset

    import spark.implicits._
    implicit val featureSwitches: FeatureSwitches = featureSwitchesOverride.getOrElse(FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMR)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setOriginalColumnsMutability(originalColumnsMutability))


    val conformed = DynamicInterpreter.interpret(inputDataset, inputDf)

    val conformedJSON = conformed.orderBy($"id").toJSON.collect().mkString("\n")

    if (conformedJSON != expectedJSON) {
      logger.error("EXPECTED:")
      logger.error(expectedJSON)
      logger.error("ACTUAL:")
      logger.error(conformedJSON)
      logger.error("DETAILS (Input):")
      logDataFrameContent(inputDf, ERROR)
      logger.error("DETAILS (Conformed):")
      logDataFrameContent(conformed, ERROR)
      fail("Actual conformed dataset JSON does not match the expected JSON (see log).")
    }

  }

}
