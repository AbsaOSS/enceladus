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

package za.co.absa.enceladus.model

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.model.conformanceRule.LiteralConformanceRule

class DatasetTest extends AnyFunSuite {
  private val conformanceRule1 = LiteralConformanceRule(
    order = 0,
    controlCheckpoint = true,
    outputColumn = "something",
    value = "1.01"
  )

  private val dataset = Dataset(name = "Test DS",
    version = 1,
    hdfsPath = "newPath",
    hdfsPublishPath = "newPublishPath",
    schemaName = "newSchema",
    schemaVersion = 1,
    conformance = List(conformanceRule1),
    properties = Some(Map(
      "property1" -> "value1",
      "property2.sub" -> "value2"
      )
    ))

  private val modelVersion = ModelVersion

  private val expectedDataset =
    s"""{"metadata":{"exportVersion":$modelVersion},"item":{"name":"Test DS","hdfsPath":"newPath","hdfsPublishPath":"newPublishPath",
      |"schemaName":"newSchema","schemaVersion":1,"conformance":[{"_t":"LiteralConformanceRule","order":0,
      |"outputColumn":"something","controlCheckpoint":true,"value":"1.01"}],
      |"properties":{"property1":"value1","property2.sub":"value2"}}}""".stripMargin.replaceAll("[\\r\\n]", "")

  test("export Dataset") {
    assert(dataset.exportItem() == expectedDataset)
  }

}
