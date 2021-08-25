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

package za.co.absa.enceladus.conformance.streaming

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.interpreter.fixtures.{NestedStructsFixture, StreamingFixture}

class HyperConformanceIntegrationSuite extends AnyFunSuite with StreamingFixture with NestedStructsFixture {

  test("Test with catalyst workaround, literal factory") {
    implicit val infoDateFactory: InfoDateFactory = new InfoDateLiteralFactory("2020-05-23")
    implicit val infoVersionFactory: InfoVersionFactory = new InfoVersionLiteralFactory(1)
    val df: DataFrame = testHyperConformance(standardizedDf,
      "result",
      nestedStructsDS)
      .orderBy("ID")

    assertResult(df.count())(20)
    val conformed = spark.read
      .textFile("src/test/testData/nestedStructs/conformed_literal.json")
      .collect().mkString("\n")
    val returned = df.toJSON.collect().mkString("\n")

    assertResult(returned)(conformed)
  }

  test("Test Hyperconformance from config, column info") {
    val df: DataFrame = testHyperConformanceFromConfig(standardizedDf,
      "result",
      nestedStructsDS,
      reportDate = "2020-05-23" )
      .orderBy("ID")

    assertResult(df.count())(20)
    val conformed = spark.read
      .textFile("src/test/testData/nestedStructs/conformed_literal_info_col.json")
      .collect().mkString("\n")
    val returned = df.toJSON.collect().mkString("\n")

    assertResult(returned)(conformed)
  }

  test("Test with catalyst workaround, event time factory") {
    implicit val infoDateFactory: InfoDateFactory = new InfoDateFromColumnFactory("dates.date_format5",
      "MM-dd-yyyy HH:mm")
    implicit val infoVersionFactory: InfoVersionFactory = new InfoVersionLiteralFactory(1)
    val df: DataFrame = testHyperConformance(standardizedDf,
      "result2",
      nestedStructsDS)
      .orderBy("ID")

    assertResult(df.count())(20)
    val conformed = spark.read
      .textFile("src/test/testData/nestedStructs/conformed_eventtime.json")
      .collect().mkString("\n")
    val returned = df.toJSON.collect().mkString("\n")
    assertResult(returned)(conformed)
  }

  //should run indefinetely
  /*test("Test without catalyst workaround") {
  implicit val infoDateFactory: InfoDateFactory = new InfoDateLiteralFactory("2020-05-23")
    val frame: DataFrame = testHyperConformance(standardizedDf,
      "result2",
      nestedStructsDS,
      catalystWorkaround = false)
    frame.show()
    assertResult(frame.count())(0)
  }*/
}
