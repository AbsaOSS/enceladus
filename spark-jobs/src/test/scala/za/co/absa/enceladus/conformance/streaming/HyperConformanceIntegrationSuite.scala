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
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.interpreter.fixtures.{NestedStructsFixture, StreamingFixture}

class HyperConformanceIntegrationSuite extends FunSuite with StreamingFixture with NestedStructsFixture {

  test("Test with catalyst workaround") {
    val frame: DataFrame = testHyperConformance(standardizedDf,
      "result",
      nestedStructsDS)
    frame.show()
    assertResult(frame.count())(20)
  }

  test("Test without catalyst workaround") {
    val frame: DataFrame = testHyperConformance(standardizedDf,
      "result2",
      nestedStructsDS,
      catalystWorkaround = false)
    frame.show()
    assertResult(frame.count())(0)
  }
}
