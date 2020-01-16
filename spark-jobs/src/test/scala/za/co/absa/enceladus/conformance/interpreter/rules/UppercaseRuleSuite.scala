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
import za.co.absa.enceladus.conformance.samples.DeepArraySamples
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.UppercaseConformanceRule
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class UppercaseRuleSuite extends FunSuite with SparkTestBase with TestRuleBehaviors {
  // scalastyle:off line.size.limit

  private val uppercaseRule = UppercaseConformanceRule(order = 1, outputColumn = "ConformedName", controlCheckpoint = false, inputColumn = "name")
  private val uppercaseArrayRule = UppercaseConformanceRule(order = 2, outputColumn = "items.ConformedItemId", controlCheckpoint = false, inputColumn = "items.itemid")
  private val uppercaseDeepArrayRule = UppercaseConformanceRule(order = 3, outputColumn = "items.payments.ConformedPayId", controlCheckpoint = false, inputColumn = "items.payments.payid")

  private val uppercaseRulesList1 = List(uppercaseRule)
  private val uppercaseRulesList2 = List(uppercaseRule, uppercaseArrayRule)
  private val uppercaseRulesList3 = List(uppercaseRule, uppercaseArrayRule, uppercaseDeepArrayRule)

  private val uppercaseOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList1)

  private val uppercaseOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList2)

  private val uppercaseOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList3)

  private val conformedUppercaseOrdersJSON1: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"ConformedName":"FIFTHS ORDER"}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedUppercaseOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"ConformedItemId":"AR229"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"ConformedItemId":"2891K"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":"31239"}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"ConformedItemId":"AKUYDG"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"JUA1K0"}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"ConformedItemId":"GSHJ1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"ConformedItemId":"JDHA2"}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"ConformedItemId":"DLDA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"ConformedItemId":"D2DHJ"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"MSKA0"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":"GDAL1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":"DAKL1"}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"ConformedItemId":"HDUS1J"}],"errCol":[],"ConformedName":"FIFTHS ORDER"}"""
      .stripMargin.replace("\r\n", "\n")

  private val conformedUppercaseOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"ConformedPayId":"PID10"}],"ConformedItemId":"AR229"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"ConformedPayId":"ZK20"}],"ConformedItemId":"2891K"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":"31239"}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"ConformedPayId":"D101"},{"payid":"d102","amount":20.0,"ConformedPayId":"D102"}],"ConformedItemId":"AKUYDG"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"JUA1K0"}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":5000.0,"ConformedPayId":"PID10"}],"ConformedItemId":"GSHJ1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"ConformedPayId":"ZK20"},{"payid":"pid10","amount":2000.0,"ConformedPayId":"PID10"}],"ConformedItemId":"JDHA2"}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":"PID10"}],"ConformedItemId":"DLDA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"ConformedPayId":"ZK20"}],"ConformedItemId":"D2DHJ"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"MSKA0"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":"GDAL1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":"DAKL1"}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":11.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":12.0,"ConformedPayId":"PID10"}],"ConformedItemId":"HDUS1J"}],"errCol":[],"ConformedName":"FIFTHS ORDER"}"""
      .stripMargin.replace("\r\n", "\n")

  private val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.ordersData)

  test("Uppercase conformance rule test 1") {
    conformanceRuleShouldMatchExpected(inputDf, uppercaseOrdersDS1, conformedUppercaseOrdersJSON1)
  }

  test("Uppercase conformance rule test 2") {
    conformanceRuleShouldMatchExpected(inputDf, uppercaseOrdersDS2, conformedUppercaseOrdersJSON2)
  }

  test("Uppercase conformance rule test 3") {
    conformanceRuleShouldMatchExpected(inputDf, uppercaseOrdersDS3, conformedUppercaseOrdersJSON3)
  }

}
