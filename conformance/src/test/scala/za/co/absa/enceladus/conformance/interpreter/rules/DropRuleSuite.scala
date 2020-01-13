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
import org.scalatest.FunSuite
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.DropConformanceRule
import za.co.absa.enceladus.samples.DeepArraySamples
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class DropRuleSuite extends FunSuite with SparkTestBase with TestRuleBehaviors {
  // scalastyle:off line.size.limit

  private val dropRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "name" )
  private val dropArrayRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "items" )
  private val dropDeepArrayRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "items.payments" )
  private val dropNonExisting = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "badName" )

  private val dropOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropRule))

  private val dropOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropArrayRule))

  private val dropOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropDeepArrayRule))

  private val dropOrdersDS4 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropNonExisting))


  private val conformedDropOrdersJSON1: String =
    """{"id":1,"items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":2,"items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":3,"items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[]}
      |{"id":4,"items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[]}
      |{"id":5,"items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[]}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedDropOrdersJSON2: String =
    """{"id":1,"name":"First Order","errCol":[]}
      |{"id":2,"name":"Second Order","errCol":[]}
      |{"id":3,"name":"Third Order","errCol":[]}
      |{"id":4,"name":"Fourth Order","errCol":[]}
      |{"id":5,"name":"Fifths order","errCol":[]}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedDropOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1},{"itemid":"2891k","qty":100,"price":1.1},{"itemid":"31239","qty":2,"price":55.2}],"errCol":[]}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0},{"itemid":"jUa1k0","qty":2,"price":55.2}],"errCol":[]}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0},{"itemid":"Jdha2","qty":100,"price":45.0}],"errCol":[]}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1},{"itemid":"d2dhJ","qty":100,"price":1.1},{"itemid":"Mska0","qty":2,"price":55.2},{"itemid":"Gdal1","qty":20,"price":5.2},{"itemid":"dakl1","qty":99,"price":1.2}],"errCol":[]}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2}],"errCol":[]}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedDropOrdersJSON4: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[]}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[]}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[]}"""
      .stripMargin.replace("\r\n", "\n")

  private val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.ordersData)

  test("Drop conformance rule test 1 - drop root column") {
    conformanceRuleShouldMatchExpected(inputDf, dropOrdersDS1, conformedDropOrdersJSON1)
  }

  test("Drop conformance rule test 2 - drop root array") {
    conformanceRuleShouldMatchExpected(inputDf, dropOrdersDS2, conformedDropOrdersJSON2)
  }

  test("Drop conformance rule test 3 - drop deep array") {
    conformanceRuleShouldMatchExpected(inputDf, dropOrdersDS3, conformedDropOrdersJSON3)
  }

  test("Drop conformance rule test 4 - column name does not exist") {
    conformanceRuleShouldMatchExpected(inputDf, dropOrdersDS4, conformedDropOrdersJSON4)
  }
}
