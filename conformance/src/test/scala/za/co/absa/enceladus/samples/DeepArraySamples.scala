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
import za.co.absa.enceladus.model.conformanceRule.{CastingConformanceRule, UppercaseConformanceRule}

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
  val uppercaseArrayRule = UppercaseConformanceRule(order = 1, outputColumn = "items.ConformedItemId", controlCheckpoint = false, inputColumn = "items.itemid")
  val uppercaseDeepArrayRule = UppercaseConformanceRule(order = 1, outputColumn = "items.payments.ConformedPayId", controlCheckpoint = false, inputColumn = "items.payments.payid")

  val rulesList1 = List(uppercaseRule)
  val rulesList2 = List(uppercaseRule, uppercaseArrayRule)
  val rulesList3 = List(uppercaseRule, uppercaseArrayRule, uppercaseDeepArrayRule)

  val ordersDS1 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = rulesList1)

  val ordersDS2 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = rulesList2)

  val ordersDS3 = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = rulesList3)


  val conformedOrdersJSON1: String = "{\"id\":1,\"name\":\"First Order\",\"items\":[{\"itemid\":\"ar229\",\"qty\":10,\"price\":5.1," +
    "\"payments\":[{\"payid\":\"pid10\",\"amount\":51.0}]},{\"itemid\":\"2891k\",\"qty\":100,\"price\":1.1,\"payments\":[{\"payid\":\"zk20\"," +
    "\"amount\":100.0}]},{\"itemid\":\"31239\",\"qty\":2,\"price\":55.2,\"payments\":[]}],\"errCol\":[],\"ConformedName\":\"FIRST ORDER\"}\n" +
    "{\"id\":2,\"name\":\"Second Order\",\"items\":[{\"itemid\":\"AkuYdg\",\"qty\":100,\"price\":10.0,\"payments\":[{\"payid\":\"d101\"," +
    "\"amount\":10.0},{\"payid\":\"d102\",\"amount\":20.0}]},{\"itemid\":\"jUa1k0\",\"qty\":2,\"price\":55.2,\"payments\":[]}]," +
    "\"errCol\":[],\"ConformedName\":\"SECOND ORDER\"}\n{\"id\":3,\"name\":\"Third Order\",\"items\":[{\"itemid\":\"Gshj1\"," +
    "\"qty\":10,\"price\":10000.0,\"payments\":[{\"payid\":\"pid10\",\"amount\":2000.0},{\"payid\":\"pid10\",\"amount\":5000.0}]}," +
    "{\"itemid\":\"Jdha2\",\"qty\":100,\"price\":45.0,\"payments\":[{\"payid\":\"zk20\",\"amount\":150.0},{\"payid\":\"pid10\"," +
    "\"amount\":2000.0}]}],\"errCol\":[],\"ConformedName\":\"THIRD ORDER\"}\n{\"id\":4,\"name\":\"Fourth Order\"," +
    "\"items\":[{\"itemid\":\"dLda1\",\"qty\":10,\"price\":5.1,\"payments\":[{\"payid\":\"pid10\",\"amount\":10.0}]}," +
    "{\"itemid\":\"d2dhJ\",\"qty\":100,\"price\":1.1,\"payments\":[{\"payid\":\"zk20\",\"amount\":15.0}]},{\"itemid\":\"Mska0\"," +
    "\"qty\":2,\"price\":55.2,\"payments\":[]},{\"itemid\":\"Gdal1\",\"qty\":20,\"price\":5.2,\"payments\":[]},{\"itemid\":\"dakl1\"," +
    "\"qty\":99,\"price\":1.2,\"payments\":[]}],\"errCol\":[],\"ConformedName\":\"FOURTH ORDER\"}\n{\"id\":5,\"name\":\"Fifths order\"," +
    "\"items\":[{\"itemid\":\"hdUs1J\",\"qty\":50,\"price\":0.2,\"payments\":[{\"payid\":\"pid10\",\"amount\":10.0},{\"payid\":\"pid10\"," +
    "\"amount\":11.0},{\"payid\":\"pid10\",\"amount\":12.0}]}],\"errCol\":[],\"ConformedName\":\"FIFTHS ORDER\"}"

  val conformedOrdersJSON2: String = "{\"id\":1,\"name\":\"First Order\",\"items\":[{\"itemid\":\"ar229\",\"qty\":10,\"price\":5.1," +
    "\"payments\":[{\"payid\":\"pid10\",\"amount\":51.0}],\"ConformedItemId\":\"AR229\"},{\"itemid\":\"2891k\",\"qty\":100" +
    ",\"price\":1.1,\"payments\":[{\"payid\":\"zk20\",\"amount\":100.0}],\"ConformedItemId\":\"2891K\"},{\"itemid\":\"31239\"" +
    ",\"qty\":2,\"price\":55.2,\"payments\":[],\"ConformedItemId\":\"31239\"}],\"errCol\":[],\"ConformedName\":\"FIRST ORDER\"}\n" +
    "{\"id\":2,\"name\":\"Second Order\",\"items\":[{\"itemid\":\"AkuYdg\",\"qty\":100,\"price\":10.0,\"payments\":[{\"payid\":\"d101\"" +
    ",\"amount\":10.0},{\"payid\":\"d102\",\"amount\":20.0}],\"ConformedItemId\":\"AKUYDG\"},{\"itemid\":\"jUa1k0\",\"qty\":2," +
    "\"price\":55.2,\"payments\":[],\"ConformedItemId\":\"JUA1K0\"}],\"errCol\":[],\"ConformedName\":\"SECOND ORDER\"}\n{\"id\":3," +
    "\"name\":\"Third Order\",\"items\":[{\"itemid\":\"Gshj1\",\"qty\":10,\"price\":10000.0,\"payments\":[{\"payid\":\"pid10\"," +
    "\"amount\":2000.0},{\"payid\":\"pid10\",\"amount\":5000.0}],\"ConformedItemId\":\"GSHJ1\"},{\"itemid\":\"Jdha2\",\"qty\":100," +
    "\"price\":45.0,\"payments\":[{\"payid\":\"zk20\",\"amount\":150.0},{\"payid\":\"pid10\",\"amount\":2000.0}],\"ConformedItemId\":" +
    "\"JDHA2\"}],\"errCol\":[],\"ConformedName\":\"THIRD ORDER\"}\n{\"id\":4,\"name\":\"Fourth Order\",\"items\":[{\"itemid\":\"dLda1\"," +
    "\"qty\":10,\"price\":5.1,\"payments\":[{\"payid\":\"pid10\",\"amount\":10.0}],\"ConformedItemId\":\"DLDA1\"},{\"itemid\":\"d2dhJ\"," +
    "\"qty\":100,\"price\":1.1,\"payments\":[{\"payid\":\"zk20\",\"amount\":15.0}],\"ConformedItemId\":\"D2DHJ\"},{\"itemid\":\"Mska0\"," +
    "\"qty\":2,\"price\":55.2,\"payments\":[],\"ConformedItemId\":\"MSKA0\"},{\"itemid\":\"Gdal1\",\"qty\":20,\"price\":5.2,\"payments\":[]" +
    ",\"ConformedItemId\":\"GDAL1\"},{\"itemid\":\"dakl1\",\"qty\":99,\"price\":1.2,\"payments\":[],\"ConformedItemId\":\"DAKL1\"}]" +
    ",\"errCol\":[],\"ConformedName\":\"FOURTH ORDER\"}\n{\"id\":5,\"name\":\"Fifths order\",\"items\":[{\"itemid\":\"hdUs1J\",\"qty\":50" +
    ",\"price\":0.2,\"payments\":[{\"payid\":\"pid10\",\"amount\":10.0},{\"payid\":\"pid10\",\"amount\":11.0},{\"payid\":\"pid10\"" +
    ",\"amount\":12.0}],\"ConformedItemId\":\"HDUS1J\"}],\"errCol\":[],\"ConformedName\":\"FIFTHS ORDER\"}"

  val conformedOrdersJSON3: String = "{\"id\":1,\"name\":\"First Order\",\"items\":[{\"itemid\":\"ar229\",\"qty\":10," +
    "\"price\":5.1,\"payments\":[{\"payid\":\"pid10\",\"amount\":51.0,\"ConformedPayId\":\"PID10\"}],\"ConformedItemId\":\"AR229\"}," +
    "{\"itemid\":\"2891k\",\"qty\":100,\"price\":1.1,\"payments\":[{\"payid\":\"zk20\",\"amount\":100.0,\"ConformedPayId\":\"ZK20\"}]," +
    "\"ConformedItemId\":\"2891K\"},{\"itemid\":\"31239\",\"qty\":2,\"price\":55.2,\"payments\":[],\"ConformedItemId\":\"31239\"}]," +
    "\"errCol\":[],\"ConformedName\":\"FIRST ORDER\"}\n{\"id\":2,\"name\":\"Second Order\",\"items\":[{\"itemid\":\"AkuYdg\",\"qty\":100," +
    "\"price\":10.0,\"payments\":[{\"payid\":\"d101\",\"amount\":10.0,\"ConformedPayId\":\"D101\"},{\"payid\":\"d102\",\"amount\":20.0," +
    "\"ConformedPayId\":\"D102\"}],\"ConformedItemId\":\"AKUYDG\"},{\"itemid\":\"jUa1k0\",\"qty\":2,\"price\":55.2,\"payments\":[]," +
    "\"ConformedItemId\":\"JUA1K0\"}],\"errCol\":[],\"ConformedName\":\"SECOND ORDER\"}\n{\"id\":3,\"name\":\"Third Order\"," +
    "\"items\":[{\"itemid\":\"Gshj1\",\"qty\":10,\"price\":10000.0,\"payments\":[{\"payid\":\"pid10\",\"amount\":2000.0,\"" +
    "ConformedPayId\":\"PID10\"},{\"payid\":\"pid10\",\"amount\":5000.0,\"ConformedPayId\":\"PID10\"}],\"ConformedItemId\":\"GSHJ1\"}," +
    "{\"itemid\":\"Jdha2\",\"qty\":100,\"price\":45.0,\"payments\":[{\"payid\":\"zk20\",\"amount\":150.0,\"ConformedPayId\":\"ZK20\"}," +
    "{\"payid\":\"pid10\",\"amount\":2000.0,\"ConformedPayId\":\"PID10\"}],\"ConformedItemId\":\"JDHA2\"}],\"errCol\":[]," +
    "\"ConformedName\":\"THIRD ORDER\"}\n{\"id\":4,\"name\":\"Fourth Order\",\"items\":[{\"itemid\":\"dLda1\",\"qty\":10," +
    "\"price\":5.1,\"payments\":[{\"payid\":\"pid10\",\"amount\":10.0,\"ConformedPayId\":\"PID10\"}],\"ConformedItemId\":\"DLDA1\"}," +
    "{\"itemid\":\"d2dhJ\",\"qty\":100,\"price\":1.1,\"payments\":[{\"payid\":\"zk20\",\"amount\":15.0,\"ConformedPayId\":\"ZK20\"}]," +
    "\"ConformedItemId\":\"D2DHJ\"},{\"itemid\":\"Mska0\",\"qty\":2,\"price\":55.2,\"payments\":[],\"ConformedItemId\":\"MSKA0\"}," +
    "{\"itemid\":\"Gdal1\",\"qty\":20,\"price\":5.2,\"payments\":[],\"ConformedItemId\":\"GDAL1\"},{\"itemid\":\"dakl1\"," +
    "\"qty\":99,\"price\":1.2,\"payments\":[],\"ConformedItemId\":\"DAKL1\"}],\"errCol\":[],\"ConformedName\":\"FOURTH ORDER\"}\n" +
    "{\"id\":5,\"name\":\"Fifths order\",\"items\":[{\"itemid\":\"hdUs1J\",\"qty\":50,\"price\":0.2,\"payments\":[{\"payid\":\"pid10\"," +
    "\"amount\":10.0,\"ConformedPayId\":\"PID10\"},{\"payid\":\"pid10\",\"amount\":11.0,\"ConformedPayId\":\"PID10\"}," +
    "{\"payid\":\"pid10\",\"amount\":12.0,\"ConformedPayId\":\"PID10\"}],\"ConformedItemId\":\"HDUS1J\"}],\"errCol\":[]," +
    "\"ConformedName\":\"FIFTHS ORDER\"}"
}
