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

package za.co.absa.enceladus.samples

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule._

object DeepArraySamples {
  case class Payment(payid: String, amount: Double)

  case class OrderItem(itemid: String, qty: Int, price: Double, payments: Seq[Payment])

  case class Order(id: Long, name: String, items: Seq[OrderItem])

  val ordersData: Seq[Order] = Seq[Order](
    Order(1L, "First Order", Seq[OrderItem](
      OrderItem("ar229", 10, 5.1, Seq(Payment("pid10", 51.0))),
      OrderItem("2891k", 100, 1.1, Seq(Payment("zk20", 100.0))),
      OrderItem("31239", 2, 55.2, Nil)
    )),
      Order(2L, "Second Order", Seq[OrderItem](
      OrderItem("AkuYdg", 100, 10, Seq(Payment("d101", 10.0), Payment("d102", 20.0))),
      OrderItem("jUa1k0", 2, 55.2, Nil)
    )),
      Order(3L, "Third Order", Seq[OrderItem](
      OrderItem("Gshj1", 10, 10000, Seq(Payment("pid10", 2000.0), Payment("pid10", 5000.0))),
      OrderItem("Jdha2", 100, 45, Seq(Payment("zk20", 150.0), Payment("pid10", 2000.0)))
    )),
      Order(4L, "Fourth Order", Seq[OrderItem](
      OrderItem("dLda1", 10, 5.1, Seq(Payment("pid10", 10.0))),
      OrderItem("d2dhJ", 100, 1.1, Seq(Payment("zk20", 15.0))),
      OrderItem("Mska0", 2, 55.2, Nil),
      OrderItem("Gdal1", 20, 5.2, Nil),
      OrderItem("dakl1", 99, 1.2, Nil)
    )),
      Order(5L, "Fifths order", Seq[OrderItem](
      OrderItem("hdUs1J", 50, 0.2, Seq(Payment("pid10", 10.0), Payment("pid10", 11.0), Payment("pid10", 12.0)))
    ))
  )

  val ordersSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("items", ArrayType(StructType(Array(
        StructField("itemid", StringType, nullable = false),
        StructField("qty", IntegerType, nullable = false),
        StructField("price", DecimalType(18, 10), nullable = false),
        StructField("payments", ArrayType(StructType(Array(
          StructField("payid", StringType, nullable = false),
          StructField("amount", DecimalType(18, 10), nullable = false)
        )))) // payments
      )))) // items
    ))

  val uppercaseRule = UppercaseConformanceRule(order = 1, outputColumn = "ConformedName", controlCheckpoint = false, inputColumn = "name")
  val uppercaseArrayRule = UppercaseConformanceRule(order = 2, outputColumn = "items.ConformedItemId", controlCheckpoint = false, inputColumn = "items.itemid")
  val uppercaseDeepArrayRule = UppercaseConformanceRule(order = 3, outputColumn = "items.payments.ConformedPayId", controlCheckpoint = false, inputColumn = "items.payments.payid")

  val uppercaseRulesList1 = List(uppercaseRule)
  val uppercaseRulesList2 = List(uppercaseRule, uppercaseArrayRule)
  val uppercaseRulesList3 = List(uppercaseRule, uppercaseArrayRule, uppercaseDeepArrayRule)

  val uppercaseOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList1)

  val uppercaseOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList2)

  val uppercaseOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = uppercaseRulesList3)

  val conformedUppercaseOrdersJSON1: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"ConformedName":"FIFTHS ORDER"}""".stripMargin

  val conformedUppercaseOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"ConformedItemId":"AR229"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"ConformedItemId":"2891K"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":"31239"}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"ConformedItemId":"AKUYDG"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"JUA1K0"}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"ConformedItemId":"GSHJ1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"ConformedItemId":"JDHA2"}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"ConformedItemId":"DLDA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"ConformedItemId":"D2DHJ"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"MSKA0"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":"GDAL1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":"DAKL1"}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"ConformedItemId":"HDUS1J"}],"errCol":[],"ConformedName":"FIFTHS ORDER"}""".stripMargin

  val conformedUppercaseOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"ConformedPayId":"PID10"}],"ConformedItemId":"AR229"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"ConformedPayId":"ZK20"}],"ConformedItemId":"2891K"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":"31239"}],"errCol":[],"ConformedName":"FIRST ORDER"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"ConformedPayId":"D101"},{"payid":"d102","amount":20.0,"ConformedPayId":"D102"}],"ConformedItemId":"AKUYDG"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"JUA1K0"}],"errCol":[],"ConformedName":"SECOND ORDER"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":5000.0,"ConformedPayId":"PID10"}],"ConformedItemId":"GSHJ1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"ConformedPayId":"ZK20"},{"payid":"pid10","amount":2000.0,"ConformedPayId":"PID10"}],"ConformedItemId":"JDHA2"}],"errCol":[],"ConformedName":"THIRD ORDER"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":"PID10"}],"ConformedItemId":"DLDA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"ConformedPayId":"ZK20"}],"ConformedItemId":"D2DHJ"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":"MSKA0"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":"GDAL1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":"DAKL1"}],"errCol":[],"ConformedName":"FOURTH ORDER"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":11.0,"ConformedPayId":"PID10"},{"payid":"pid10","amount":12.0,"ConformedPayId":"PID10"}],"ConformedItemId":"HDUS1J"}],"errCol":[],"ConformedName":"FIFTHS ORDER"}""".stripMargin


  val literalRule = LiteralConformanceRule(order = 1, outputColumn = "System", controlCheckpoint = false, value = "FA")
  val literalArrayRule = LiteralConformanceRule(order = 2, outputColumn = "items.SubSystem", controlCheckpoint = false, value = "Orders")
  val literalDeepArrayRule = LiteralConformanceRule(order = 3, outputColumn = "items.payments.SubSubSystem", controlCheckpoint = false, value = "Trade")

  val literalRulesList1 = List(literalRule)
  val literalRulesList2 = List(literalRule, literalArrayRule)
  val literalRulesList3 = List(literalRule, literalArrayRule, literalDeepArrayRule)

  val literalOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList1)

  val literalOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList2)

  val literalOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = literalRulesList3)


  val conformedLiteralOrdersJSON1: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"System":"FA"}""".stripMargin

  val conformedLiteralOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"SubSystem":"Orders"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"SubSystem":"Orders"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"SubSystem":"Orders"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"SubSystem":"Orders"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"SubSystem":"Orders"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"SubSystem":"Orders"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"Orders"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}""".stripMargin

  val conformedLiteralOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"SubSubSystem":"Trade"},{"payid":"d102","amount":20.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":5000.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":2000.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"Orders"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"Orders"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":11.0,"SubSubSystem":"Trade"},{"payid":"pid10","amount":12.0,"SubSubSystem":"Trade"}],"SubSystem":"Orders"}],"errCol":[],"System":"FA"}""".stripMargin

  val dropRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "name" )
  val dropArrayRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "name" )
  val dropDeepArrayRule = DropConformanceRule(order = 1, controlCheckpoint = false, outputColumn = "name" )

  val dropOrdersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropRule))

  val dropOrdersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropArrayRule))

  val dropOrdersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(dropDeepArrayRule))


  val conformedDropOrdersJSON1: String =
    """{"id":1,"items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":2,"items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":3,"items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[]}
      |{"id":4,"items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[]}
      |{"id":5,"items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[]}""".stripMargin

  val conformedDropOrdersJSON2: String =
    """{"id":1,"items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":2,"items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":3,"items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[]}
      |{"id":4,"items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[]}
      |{"id":5,"items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[]}""".stripMargin

  val conformedDropOrdersJSON3: String =
    """{"id":1,"items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}]},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}]},{"itemid":"31239","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":2,"items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}]},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[]}],"errCol":[]}
      |{"id":3,"items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}]},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}]}],"errCol":[]}
      |{"id":4,"items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}]},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}]},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[]},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[]},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[]}],"errCol":[]}
      |{"id":5,"items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[]}""".stripMargin

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
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}]}],"errCol":[],"conformedId":{"id2":5}}""".stripMargin

  val conformedSingleColumnOrdersJSON2: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"ConformedItemId":{"itemid3":"ar229"}},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"ConformedItemId":{"itemid3":"2891k"}},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"31239"}}],"errCol":[],"conformedId":{"id2":1}}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"ConformedItemId":{"itemid3":"AkuYdg"}},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"jUa1k0"}}],"errCol":[],"conformedId":{"id2":2}}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"ConformedItemId":{"itemid3":"Gshj1"}},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"ConformedItemId":{"itemid3":"Jdha2"}}],"errCol":[],"conformedId":{"id2":3}}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"ConformedItemId":{"itemid3":"dLda1"}},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"ConformedItemId":{"itemid3":"d2dhJ"}},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"Mska0"}},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":{"itemid3":"Gdal1"}},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":{"itemid3":"dakl1"}}],"errCol":[],"conformedId":{"id2":4}}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"ConformedItemId":{"itemid3":"hdUs1J"}}],"errCol":[],"conformedId":{"id2":5}}""".stripMargin

  val conformedSingleColumnOrdersJSON3: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"ar229"}},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"ConformedPayId":{"payid3":"zk20"}}],"ConformedItemId":{"itemid3":"2891k"}},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"31239"}}],"errCol":[],"conformedId":{"id2":1}}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"ConformedPayId":{"payid3":"d101"}},{"payid":"d102","amount":20.0,"ConformedPayId":{"payid3":"d102"}}],"ConformedItemId":{"itemid3":"AkuYdg"}},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"jUa1k0"}}],"errCol":[],"conformedId":{"id2":2}}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":5000.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"Gshj1"}},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"ConformedPayId":{"payid3":"zk20"}},{"payid":"pid10","amount":2000.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"Jdha2"}}],"errCol":[],"conformedId":{"id2":3}}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"dLda1"}},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"ConformedPayId":{"payid3":"zk20"}}],"ConformedItemId":{"itemid3":"d2dhJ"}},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"ConformedItemId":{"itemid3":"Mska0"}},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"ConformedItemId":{"itemid3":"Gdal1"}},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"ConformedItemId":{"itemid3":"dakl1"}}],"errCol":[],"conformedId":{"id2":4}}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":11.0,"ConformedPayId":{"payid3":"pid10"}},{"payid":"pid10","amount":12.0,"ConformedPayId":{"payid3":"pid10"}}],"ConformedItemId":{"itemid3":"hdUs1J"}}],"errCol":[],"conformedId":{"id2":5}}""".stripMargin

  val sparkSessionRule = SparkSessionConfConformanceRule(order = 1, outputColumn = "TimeZone", controlCheckpoint = false, sparkConfKey = "spark.sql.session.timeZone")
  val sparkSessionArrayRule = SparkSessionConfConformanceRule(order = 2, outputColumn = "items.SubSystem", controlCheckpoint = false, sparkConfKey = "spark.sql.session.mySubSystem")
  val sparkSessionDeepArrayRule = SparkSessionConfConformanceRule(order = 3, outputColumn = "items.payments.SubSubSystem", controlCheckpoint = false, sparkConfKey = "spark.sql.session.mySubSubSystem")

  val sparkSessionRulesList = List(sparkSessionRule, sparkSessionArrayRule, sparkSessionDeepArrayRule)

  val sparkSessionOrdersDS = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = sparkSessionRulesList)

  val conformedSparkSessionOrdersJSON: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"GMT"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"SubSubSystem":"FA2"},{"payid":"d102","amount":20.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"GMT"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":5000.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":2000.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"GMT"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"FA1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"GMT"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":11.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":12.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"GMT"}""".stripMargin

}
