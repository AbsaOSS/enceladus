/*
 * Copyright 2018-2019 ABSA Group Limited
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
import za.co.absa.enceladus.model.conformanceRule.LiteralConformanceRule
import za.co.absa.enceladus.samples.DeepArraySamples
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class LiteralRuleSuite extends FunSuite with SparkTestBase with TestRuleBehaviors {

  private val literalRule = LiteralConformanceRule(order = 1, outputColumn = "System", controlCheckpoint = false, value = "FA")
  private val literalArrayRule = LiteralConformanceRule(order = 2, outputColumn = "items.SubSystem", controlCheckpoint = false, value = "Orders")
  private val literalDeepArrayRule = LiteralConformanceRule(order = 3, outputColumn = "items.payments.SubSubSystem", controlCheckpoint = false, value = "Trade")

  private val literalRulesList1 = List(literalRule)
  private val literalRulesList2 = List(literalRule, literalArrayRule)
  private val literalRulesList3 = List(literalRule, literalArrayRule, literalDeepArrayRule)

  private val literalOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList1)

  private val literalOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList2)

  private val literalOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList3)


  private val conformedLiteralOrdersJSON1: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"System":"FA"}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedLiteralOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"SubSystem":"Orders"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"SubSystem":"Orders"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"SubSystem":"Orders"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"SubSystem":"Orders"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"SubSystem":"Orders"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"SubSystem":"Orders"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"Orders"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedLiteralOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"SubSubSystem":"Trade"},{"payid":"d102","amount":20.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":5000.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":2000.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"Orders"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":11.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":12.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}"""
      .stripMargin.replace("\r\n", "\n")

  private val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.ordersData)

  test("Literal conformance rule test 1") {
    conformanceRuleShouldMatchExpected(inputDf, literalOrdersDS1, conformedLiteralOrdersJSON1)
  }

  test("Literal conformance rule test 2") {
    conformanceRuleShouldMatchExpected(inputDf, literalOrdersDS2, conformedLiteralOrdersJSON2)
  }

  test("Literal conformance rule test 3") {
    conformanceRuleShouldMatchExpected(inputDf, literalOrdersDS3, conformedLiteralOrdersJSON3)
  }

}
