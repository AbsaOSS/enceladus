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
import za.co.absa.enceladus.conformance.interpreter.fixtures.{MultipleMappingFixture, StreamingFixture}

class HyperConformanceMappingIntegrationSuite extends AnyFunSuite with StreamingFixture with MultipleMappingFixture {

  test("Test streaming multiple mapping") {
    implicit val infoDateFactory: InfoDateFactory = new InfoDateLiteralFactory("2020-05-23")
    val df: DataFrame = testHyperConformance(standardizedDf,
      "result", mappingDS)
      .orderBy("result.property")

    assertResult(3)(df.count())
    val conformed = spark.read
      .textFile("src/test/testData/multipleMapping/conformed_multiple_mapping.json")
      .collect().mkString("\n")
    val returned = df.toJSON.collect().mkString("\n")
    assertResult(returned)(conformed)
  }
}
