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
import org.mockito.Mockito.{ mock, when => mockWhen }
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.samples._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
class ArrayConformanceSuite extends FunSuite with SparkTestBase with BeforeAndAfterAll {

  import spark.implicits._
  // spark.enableControlFrameworkTracking()

  implicit var dao: EnceladusDAO = null
  implicit var progArgs: CmdConfig = null

  override def beforeAll(): Unit = {

    val mapDF = spark.createDataFrame(MappingsSamples.mapping)

    dao = mock(classOf[EnceladusDAO])
    progArgs = new CmdConfig(reportDate = "2017-11-01")

    mockWhen(dao.getMappingTable("mapping", 0)) thenReturn MappingsSamples.mappingTable

    spark.sessionState.conf.setConfString("za.co.absa.myVal", "myConf")

    DataSource.setData("mapping", mapDF)
  }

  test("Testing Array Type conformance") {

    val df = spark.createDataFrame(ArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema

    val conformedDf = DynamicInterpreter.interpret(ArraySamples.conformanceDef, df)(spark, dao, progArgs, false).cache()
    val expected = ArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[ConformedOuter].collect().sortBy(_.order).toList
    assertResult(expected)(conformed)

    conformedDf.show(false)

    val numOfErrors = conformedDf.select(size(col("errCol")).as("numErrors")).agg(sum(col("numErrors"))).collect()(0)(0).toString.toInt
    assert(numOfErrors == ArraySamples.totalNumberOfErrors)
  }

  test("Conformance should NOT generate errors when matching null ") {

    val df = spark.createDataFrame(NullArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema

    val conformedDf = DynamicInterpreter.interpret(NullArraySamples.mappingOnlyConformanceDef, df)(spark, dao, progArgs, false).cache()
    val expected = NullArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[OuterErr].collect().sortBy(_.order).toList

    assertResult(expected)(conformed)

    val numOfErrors = conformedDf.select(size(col("errCol")).as("numErrors")).agg(sum(col("numErrors"))).collect()(0)(0).toString.toInt

    assert(numOfErrors == NullArraySamples.totalNumberOfErrors)
  }

  test("Conformance should NOT generate errors when matching empty arrays") {

    import za.co.absa.enceladus.samples.EmtpyArraySamples

    val df = spark.createDataFrame(EmtpyArraySamples.testData)
    mockWhen(dao.getSchema("test", 0)) thenReturn df.schema

    val conformedDf = DynamicInterpreter.interpret(EmtpyArraySamples.mappingOnlyConformanceDef, df)(spark, dao, progArgs, false).cache()
    val expected = EmtpyArraySamples.conformedData.toArray.sortBy(_.order).toList
    val conformed = conformedDf.as[OuterErr].collect().sortBy(_.order).toList

    assertResult(expected)(conformed)

    val numOfErrors = conformedDf.select(size(col("errCol")).as("numErrors")).agg(sum(col("numErrors"))).collect()(0)(0).toString.toInt

    assert(numOfErrors == EmtpyArraySamples.totalNumberOfErrors)
  }

}