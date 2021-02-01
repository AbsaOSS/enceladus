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
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import CoalesceRuleSuite._
import za.co.absa.enceladus.conformance.samples.DeepArraySamples
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.{CoalesceConformanceRule, DropConformanceRule, LiteralConformanceRule}

object CoalesceRuleSuite {
  private case class ShopItem(id: String, itemName: String, itemDescription: String, qty: Long)

  private val shopItems: Seq[ShopItem] = Seq(
    ShopItem("adui", "Pasta", "True Italian Pasta", 100L),
    ShopItem("byuw", "Sausage", null, 10L),
    ShopItem("cnbh", null, null, 1001L),
    ShopItem("dwdf", null, "Really Spicy Paprika", 25L),
    ShopItem(null, null, null, 0L)
  )

  private val coalesceRule = CoalesceConformanceRule(
    order = 1,
    outputColumn = "whatIsIt",
    controlCheckpoint = false,
    inputColumns = Seq("itemDescription", "itemName", "id")
  )

  private val litRule =  LiteralConformanceRule(
    order = 1,
    outputColumn = "items.temp",
    controlCheckpoint = false,
    value = "coalesce fill-in value"
  )

  private val coalesceArrayRule = CoalesceConformanceRule(
    order = 2,
    outputColumn = "items.itemid2",
    controlCheckpoint = false,
    inputColumns = Seq("items.itemid", "items.temp")
  )

  private val dropRule = DropConformanceRule(
    order = 3,
    controlCheckpoint = false,
    outputColumn = "items.temp"
  )

  private val rulesList1 = List(coalesceRule)
  private val rulesList2 = List(litRule, coalesceArrayRule, dropRule)

  private val shopItemsDS = Dataset(
    name = "Shop Items Conformance",
    hdfsPath = "src/test/testData/shopItem",
    hdfsPublishPath = "testData/conformedShopItems",
    schemaName = "Orders",
    schemaVersion = 1,
    conformance = rulesList1
  )

  private val ordersDS = Dataset(
    name = "Orders Conformance",
    hdfsPath = "src/test/testData/orders",
    hdfsPublishPath = "testData/conformedOrders",
    schemaName = "Orders",
    schemaVersion = 1,
    conformance = rulesList2
  )
}

class CoalesceRuleSuite extends AnyFunSuite with SparkTestBase with TestRuleBehaviors {
  test("Coalesce conformance rule on root level fields") {
    val inputDf: DataFrame = spark.createDataFrame(shopItems)

    val expected =
    """{"qty":0,"errCol":[]}
      |{"id":"adui","itemName":"Pasta","itemDescription":"True Italian Pasta","qty":100,"errCol":[],"whatIsIt":"True Italian Pasta"}
      |{"id":"byuw","itemName":"Sausage","qty":10,"errCol":[],"whatIsIt":"Sausage"}
      |{"id":"cnbh","qty":1001,"errCol":[],"whatIsIt":"cnbh"}
      |{"id":"dwdf","itemDescription":"Really Spicy Paprika","qty":25,"errCol":[],"whatIsIt":"Really Spicy Paprika"}"""
      .stripMargin.replace("\r\n", "\n")

    conformanceRuleShouldMatchExpected(inputDf, shopItemsDS, expected)
  }

  test("Coalesce conformance rule on nested field from temporal literal field") {
    val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.ordersDataWithNulls)

    val expected =
      """{"id":1,"name":"First Order","date":"2025-11-15","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0}],"itemid2":"ar229"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0}],"itemid2":"2891k"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"itemid2":"31239"}],"errCol":[]}
        |{"id":2,"date":"2019-03-12","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0},{"payid":"d102","amount":20.0}],"itemid2":"AkuYdg"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"itemid2":"jUa1k0"}],"errCol":[]}
        |{"id":3,"name":"Third Order","items":[{"qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0},{"payid":"pid10","amount":5000.0}],"itemid2":"coalesce fill-in value"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0},{"payid":"pid10","amount":2000.0}],"itemid2":"Jdha2"}],"errCol":[]}
        |{"id":4,"name":"Fourth Order","date":"2005-01-02","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0}],"itemid2":"dLda1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0}],"itemid2":"d2dhJ"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"itemid2":"Mska0"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"itemid2":"Gdal1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"itemid2":"dakl1"}],"errCol":[]}
        |{"id":5,"name":"Fifths order","date":"2009-05-21","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0},{"payid":"pid10","amount":11.0},{"payid":"pid10","amount":12.0}],"itemid2":"hdUs1J"}],"errCol":[]}"""
        .stripMargin.replace("\r\n", "\n")

    conformanceRuleShouldMatchExpected(inputDf, ordersDS, expected)
  }
}

