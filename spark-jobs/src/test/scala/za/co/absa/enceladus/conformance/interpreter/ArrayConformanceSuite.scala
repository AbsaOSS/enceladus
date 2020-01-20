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

import org.apache.spark.sql.functions._
import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import za.co.absa.enceladus.conformance.ConfCmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.conformance.samples._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ArrayConformanceSuite extends FunSuite with SparkTestBase with BeforeAndAfterAll {

  import spark.implicits._
  // spark.enableControlFrameworkTracking()

  implicit var dao: MenasDAO = _
  implicit var progArgs: ConfCmdConfig = _

  private val enableCF = false
  private val isCatalystWorkaroundEnabled = true

  override def beforeAll(): Unit = {

    val mapDF = spark.createDataFrame(MappingsSamples.mapping)

    dao = mock(classOf[MenasDAO])
    progArgs = new ConfCmdConfig(reportDate = "2017-11-01")

    mockWhen(dao.getMappingTable("mapping", 0)) thenReturn MappingsSamples.mappingTable

    spark.sessionState.conf.setConfString("za.co.absa.myVal", "myConf")

    DataSource.setData("mapping", mapDF)
  }

  def testArrayTypeConformance(useExperimentalMappingRule: Boolean): Unit = {
    val df = spark.createDataFrame(ArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val conformedDf = DynamicInterpreter.interpret(ArraySamples.conformanceDef,
      df)
    val expected = ArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[ConformedOuter].collect().sortBy(_.order).toList
    assertResult(expected)(conformed)

    val numOfErrors = conformedDf
      .select(size(col("errCol"))
        .as("numErrors"))
      .agg(sum(col("numErrors")))
      .collect()(0)(0)
      .toString
      .toInt
    assert(numOfErrors == ArraySamples.totalNumberOfErrors)
  }

  def testConformanceMatchingNull(useExperimentalMappingRule: Boolean): Unit = {
    val df = spark.createDataFrame(NullArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val conformedDf = DynamicInterpreter.interpret(NullArraySamples.mappingOnlyConformanceDef,
      df)

    val expected = NullArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[OuterErr].collect().sortBy(_.order).toList

    assertResult(expected)(conformed)

    val numOfErrors = conformedDf
      .select(size(col("errCol"))
        .as("numErrors"))
      .agg(sum(col("numErrors")))
      .collect()(0)(0)
      .toString
      .toInt

    assert(numOfErrors == NullArraySamples.totalNumberOfErrors)
  }

  def testConformanceMatchingEmptyArrays(useExperimentalMappingRule: Boolean): Unit = {
    import za.co.absa.enceladus.conformance.samples.EmtpyArraySamples

    val df = spark.createDataFrame(EmtpyArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(useExperimentalMappingRule)
      .setCatalystWorkaroundEnabled(isCatalystWorkaroundEnabled)
      .setControlFrameworkEnabled(enableCF)
      .setBroadcastStrategyMode(Never)

    val conformedDf = DynamicInterpreter.interpret(EmtpyArraySamples.mappingOnlyConformanceDef,
      df)
    val expected = EmtpyArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[OuterErr].collect().sortBy(_.order).toList

    assertResult(expected)(conformed)

    val numOfErrors = conformedDf
      .select(size(col("errCol"))
        .as("numErrors"))
      .agg(sum(col("numErrors")))
      .collect()(0)(0)
      .toString
      .toInt

    assert(numOfErrors == EmtpyArraySamples.totalNumberOfErrors)
  }

  test("Testing Array Type conformance") {
    testArrayTypeConformance(useExperimentalMappingRule = false)
  }

  test("Testing Array Type conformance (experimental optimized mapping rule)") {
    testArrayTypeConformance(useExperimentalMappingRule = true)
  }

  test("Conformance should NOT generate errors when matching null") {
    testConformanceMatchingNull(useExperimentalMappingRule = false)
  }

  test("Conformance should NOT generate errors when matching null (experimental optimized mapping rule)") {
    testConformanceMatchingNull(useExperimentalMappingRule = true)
  }

  test("Conformance should NOT generate errors when matching empty arrays") {
    testConformanceMatchingEmptyArrays(useExperimentalMappingRule = false)
  }

  test("Conformance should NOT generate errors when matching empty arrays (experimental optimized mapping rule)") {
    testConformanceMatchingEmptyArrays(useExperimentalMappingRule = true)
  }
}
