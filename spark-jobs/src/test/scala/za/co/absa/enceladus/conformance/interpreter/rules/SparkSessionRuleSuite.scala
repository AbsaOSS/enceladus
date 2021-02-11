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

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.samples.DeepArraySamples
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.SparkSessionConfConformanceRule
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class SparkSessionRuleSuite extends AnyFunSuite with SparkTestBase with TestRuleBehaviors {
  // scalastyle:off line.size.limit

  private val sparkSessionRule = SparkSessionConfConformanceRule(order = 1, outputColumn = "TimeZone", controlCheckpoint = false, sparkConfKey = "spark.sql.session.timeZone")
  private val sparkSessionArrayRule = SparkSessionConfConformanceRule(order = 2, outputColumn = "items.SubSystem", controlCheckpoint = false, sparkConfKey = "spark.sql.session.mySubSystem")
  private val sparkSessionDeepArrayRule = SparkSessionConfConformanceRule(order = 3, outputColumn = "items.payments.SubSubSystem", controlCheckpoint = false, sparkConfKey = "spark.sql.session.mySubSubSystem")

  private val sparkSessionRulesList = List(sparkSessionRule, sparkSessionArrayRule, sparkSessionDeepArrayRule)

  private val sparkSessionOrdersDS = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = sparkSessionRulesList)

  private val conformedSparkSessionOrdersJSON: String =
    """{"id":1,"name":"First Order","items":[{"itemid":"ar229","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":51.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"2891k","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":100.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"31239","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"UTC"}
      |{"id":2,"name":"Second Order","items":[{"itemid":"AkuYdg","qty":100,"price":10.0,"payments":[{"payid":"d101","amount":10.0,"SubSubSystem":"FA2"},{"payid":"d102","amount":20.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"jUa1k0","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"UTC"}
      |{"id":3,"name":"Third Order","items":[{"itemid":"Gshj1","qty":10,"price":10000.0,"payments":[{"payid":"pid10","amount":2000.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":5000.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"Jdha2","qty":100,"price":45.0,"payments":[{"payid":"zk20","amount":150.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":2000.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"UTC"}
      |{"id":4,"name":"Fourth Order","items":[{"itemid":"dLda1","qty":10,"price":5.1,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"d2dhJ","qty":100,"price":1.1,"payments":[{"payid":"zk20","amount":15.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"},{"itemid":"Mska0","qty":2,"price":55.2,"payments":[],"SubSystem":"FA1"},{"itemid":"Gdal1","qty":20,"price":5.2,"payments":[],"SubSystem":"FA1"},{"itemid":"dakl1","qty":99,"price":1.2,"payments":[],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"UTC"}
      |{"id":5,"name":"Fifths order","items":[{"itemid":"hdUs1J","qty":50,"price":0.2,"payments":[{"payid":"pid10","amount":10.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":11.0,"SubSubSystem":"FA2"},{"payid":"pid10","amount":12.0,"SubSubSystem":"FA2"}],"SubSystem":"FA1"}],"errCol":[],"TimeZone":"UTC"}"""
      .stripMargin.replace("\r\n", "\n")


  test("Spark Session conformance rule test") {
    val inputDf = spark.createDataFrame(DeepArraySamples.ordersData)

    spark.conf.set("spark.sql.session.mySubSystem", "FA1")
    spark.conf.set("spark.sql.session.mySubSubSystem", "FA2")

    conformanceRuleShouldMatchExpected(inputDf, sparkSessionOrdersDS, conformedSparkSessionOrdersJSON)
  }

}
