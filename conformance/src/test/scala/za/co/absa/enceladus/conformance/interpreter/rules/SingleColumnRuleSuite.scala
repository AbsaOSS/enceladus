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
import za.co.absa.enceladus.model.conformanceRule.SingleColumnConformanceRule
import za.co.absa.enceladus.samples.DeepArraySamples
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class SingleColumnRuleSuite extends FunSuite with SparkTestBase with TestRuleBehaviors {

  val singleColumnRule = SingleColumnConformanceRule(order = 1, controlCheckpoint = false, "conformedId", "id", "id2")
  val singleColumnArrayRule = SingleColumnConformanceRule(order = 1, controlCheckpoint = false, "items.ConformedItemId", "items.itemid", "itemid3")
  val singleColumnDeepArrayRule = SingleColumnConformanceRule(order = 1, controlCheckpoint = false, "items.payments.ConformedPayId", "items.payments.payid", "payid3")

  val singleColumnRulesList1 = List(singleColumnRule)
  val singleColumnRulesList2 = List(singleColumnRule, singleColumnArrayRule)
  val singleColumnRulesList3 = List(singleColumnRule, singleColumnArrayRule, singleColumnDeepArrayRule)

  val singleColumnOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = singleColumnRulesList1)

  val singleColumnOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = singleColumnRulesList2)

  val singleColumnOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = singleColumnRulesList3)

  val conformedSingleColumnOrdersJSON1: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[],"conformedId":{"id2":1}}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[],"conformedId":{"id2":2}}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[],"conformedId":{"id2":3}}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[],"conformedId":{"id2":4}}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"conformedId":{"id2":5}}"""
      .stripMargin.replace("\r\n", "\n")

  val conformedSingleColumnOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"ConformedItemId":{"itemid3":"ar229"}},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"ConformedItemId":{"itemid3":"2891k"}},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"31239"}}],"errCol":[],"conformedId":{"id2":1}}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"ConformedItemId":{"itemid3":"AkuYdg"}},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"jUa1k0"}}],"errCol":[],"conformedId":{"id2":2}}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"ConformedItemId":{"itemid3":"Gshj1"}},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"ConformedItemId":{"itemid3":"Jdha2"}}],"errCol":[],"conformedId":{"id2":3}}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"ConformedItemId":{"itemid3":"dLda1"}},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"ConformedItemId":{"itemid3":"d2dhJ"}},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"Mska0"}},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":{"itemid3":"Gdal1"}},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":{"itemid3":"dakl1"}}],"errCol":[],"conformedId":{"id2":4}}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"ConformedItemId":{"itemid3":"hdUs1J"}}],"errCol":[],"conformedId":{"id2":5}}"""
      .stripMargin.replace("\r\n", "\n")

  val conformedSingleColumnOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"ar229"}},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"ConformedPayId":{"payid3":"zk20"}}],"ConformedItemId":{"itemid3":"2891k"}},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"31239"}}],"errCol":[],"conformedId":{"id2":1}}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"ConformedPayId":{"payid3":"d101"}},{"payid":"d102","amount":20.0,"ConformedPayId":{"payid3":"d102"}}],"ConformedItemId":{"itemid3":"AkuYdg"}},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"jUa1k0"}}],"errCol":[],"conformedId":{"id2":2}}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":5000.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"Gshj1"}},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"ConformedPayId":{"payid3":"zk20"}},{"payid":"pid10","amount":2000.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"Jdha2"}}],"errCol":[],"conformedId":{"id2":3}}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"dLda1"}},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"ConformedPayId":{"payid3":"zk20"}}],"ConformedItemId":{"itemid3":"d2dhJ"}},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"Mska0"}},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":{"itemid3":"Gdal1"}},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":{"itemid3":"dakl1"}}],"errCol":[],"conformedId":{"id2":4}}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":11.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":12.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"hdUs1J"}}],"errCol":[],"conformedId":{"id2":5}}"""
      .stripMargin.replace("\r\n", "\n")

  val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.ordersData)

  test("Single Column conformance rule test 1") {
    conformanceRuleShouldMatchExpected(inputDf, singleColumnOrdersDS1, conformedSingleColumnOrdersJSON1)
  }

  test("Single Column conformance rule test 2") {
    conformanceRuleShouldMatchExpected(inputDf, singleColumnOrdersDS2, conformedSingleColumnOrdersJSON2)
  }

  test("Single Column conformance rule test 3") {
    conformanceRuleShouldMatchExpected(inputDf, singleColumnOrdersDS3, conformedSingleColumnOrdersJSON3)
  }
}
