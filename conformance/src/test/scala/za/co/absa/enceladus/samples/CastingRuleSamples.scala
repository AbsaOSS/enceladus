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

package za.co.absa.enceladus.samples

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.CastingConformanceRule

object CastingRuleSamples {

  val ordersData: Seq[String] = Seq(
    """{ "id": 1, "date": "2018-02-10", "num": "888", "num2": "12", "placed": "2018-02-10T12:10:00.000Z", "discount": 0.9, "items": [
      |  { "itemid": "777", "qty": 10, "price": 12.5 },
      |  { "itemid": "2121", "qty": 15, "price": 0.77 },
      |  { "itemid": "-123", "qty": 12, "price": 1002.5 } ] }
      |
      |""".stripMargin,
    """{ "id": 2, "date": "2018-02-20", "num": "500$", "num2": "--33--", "discount": 0.95, "items": [
      |  { "itemid": "100000", "qty": 101, "price": 22.33 },
      |  { "itemid": "8282", "qty": 1, "price": 111.22 } ] }
      |
      |""".stripMargin,
    """{ "id": 3, "date": "2018-02-21", "num2": null, "placed": "2018-02-21T08:10:17.000Z", "discount": null, "items": [
      |  { "itemid": "737373", "qty": 22, "price": 59.99 },
      |  { "itemid": "28781", "qty": 10001, "price": 99.95 } ] }
      |
      |""".stripMargin,
    """{ "id": 4, "date": "2018-03-30", "num": null, "placed": "2018-03-30T00:22:10.121Z", "discount": 0.850123456789, "items": [
      |  { "itemid": "-123k", "qty": 1, "price": 33.66 } ] }
      |
      |""".stripMargin
  )

  val ordersSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("date", DateType),
      StructField("num", StringType),
      StructField("num2", StringType),
      StructField("placed", TimestampType),
      StructField("discount", DoubleType),
      StructField("items", ArrayType(StructType(Array(
        StructField("itemid", StringType, nullable = false),
        StructField("qty", IntegerType, nullable = false),
        StructField("price", DecimalType(18, 10), nullable = false)
      ))))))


  val castLongToString = CastingConformanceRule(order = 1, outputColumn = "ConformedStr", controlCheckpoint = false, inputColumn = "id",
    outputDataType = "string")
  val castDateToString = CastingConformanceRule(order = 2, outputColumn = "ConformedDate", controlCheckpoint = false, inputColumn = "date",
    outputDataType = "string")
  val castStringToLong = CastingConformanceRule(order = 3, outputColumn = "ConformedLong", controlCheckpoint = false, inputColumn = "num",
    outputDataType = "long")
  val castStringToInt = CastingConformanceRule(order = 4, outputColumn = "ConformedInt", controlCheckpoint = false, inputColumn = "num2",
    outputDataType = "integer")
  val castTimestampToLong = CastingConformanceRule(order = 5, outputColumn = "ConformedTimestamp", controlCheckpoint = false, inputColumn =
    "placed", outputDataType = "long")
  val castDoubleToDecimal = CastingConformanceRule(order = 6, outputColumn = "ConformedDecimal", controlCheckpoint = false, inputColumn =
    "discount", outputDataType = "decimal(30,6)")
  val castStringToLongArr = CastingConformanceRule(order = 7, outputColumn = "items.ConformedId", controlCheckpoint = false, inputColumn = "items" +
    ".itemid", outputDataType = "long")
  val castIntToStrArr = CastingConformanceRule(order = 8, outputColumn = "items.ConformedQty", controlCheckpoint = false, inputColumn = "items" +
    ".qty", outputDataType = "string")
  val castDecimalToDoubleArr = CastingConformanceRule(order = 9, outputColumn = "items.ConformedPrice", controlCheckpoint = false, inputColumn =
    "items.price", outputDataType = "double")

  val ordersDS = Dataset(name = "Orders Conformance", version = 1, hdfsPath = "src/test/testData/orders", hdfsPublishPath =
    "testData/conformedOrders",
    schemaName = "Orders", schemaVersion = 1,
    conformance = List(castLongToString, castDateToString, castStringToLong, castStringToInt, castTimestampToLong, castDoubleToDecimal,
      castStringToLongArr, castIntToStrArr, castDecimalToDoubleArr))

  val conformedOrdersJSON: String = """[ {
                                      |  "id" : 1,
                                      |  "date" : "2018-02-10",
                                      |  "num" : "888",
                                      |  "num2" : "12",
                                      |  "placed" : "2018-02-10T12:10:00.000Z",
                                      |  "discount" : 0.9,
                                      |  "items" : [ {
                                      |    "itemid" : "777",
                                      |    "qty" : 10,
                                      |    "price" : 12.5,
                                      |    "ConformedId" : 777,
                                      |    "ConformedQty" : "10",
                                      |    "ConformedPrice" : 12.5
                                      |  }, {
                                      |    "itemid" : "2121",
                                      |    "qty" : 15,
                                      |    "price" : 0.77,
                                      |    "ConformedId" : 2121,
                                      |    "ConformedQty" : "15",
                                      |    "ConformedPrice" : 0.77
                                      |  }, {
                                      |    "itemid" : "-123",
                                      |    "qty" : 12,
                                      |    "price" : 1002.5,
                                      |    "ConformedId" : -123,
                                      |    "ConformedQty" : "12",
                                      |    "ConformedPrice" : 1002.5
                                      |  } ],
                                      |  "errCol" : [ ],
                                      |  "ConformedStr" : "1",
                                      |  "ConformedDate" : "2018-02-10",
                                      |  "ConformedLong" : 888,
                                      |  "ConformedInt" : 12,
                                      |  "ConformedTimestamp" : 1518264600,
                                      |  "ConformedDecimal" : 0.9
                                      |}, {
                                      |  "id" : 2,
                                      |  "date" : "2018-02-20",
                                      |  "num" : "500$",
                                      |  "num2" : "--33--",
                                      |  "discount" : 0.95,
                                      |  "items" : [ {
                                      |    "itemid" : "100000",
                                      |    "qty" : 101,
                                      |    "price" : 22.33,
                                      |    "ConformedId" : 100000,
                                      |    "ConformedQty" : "101",
                                      |    "ConformedPrice" : 22.33
                                      |  }, {
                                      |    "itemid" : "8282",
                                      |    "qty" : 1,
                                      |    "price" : 111.22,
                                      |    "ConformedId" : 8282,
                                      |    "ConformedQty" : "1",
                                      |    "ConformedPrice" : 111.22
                                      |  } ],
                                      |  "errCol" : [ {
                                      |    "errType" : "confCastError",
                                      |    "errCode" : "E00003",
                                      |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
                                      |    "errCol" : "ConformedLong",
                                      |    "rawValues" : [ "500$" ],
                                      |    "mappings" : [ ]
                                      |  }, {
                                      |    "errType" : "confCastError",
                                      |    "errCode" : "E00003",
                                      |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
                                      |    "errCol" : "ConformedInt",
                                      |    "rawValues" : [ "--33--" ],
                                      |    "mappings" : [ ]
                                      |  } ],
                                      |  "ConformedStr" : "2",
                                      |  "ConformedDate" : "2018-02-20",
                                      |  "ConformedDecimal" : 0.95
                                      |}, {
                                      |  "id" : 3,
                                      |  "date" : "2018-02-21",
                                      |  "placed" : "2018-02-21T08:10:17.000Z",
                                      |  "items" : [ {
                                      |    "itemid" : "737373",
                                      |    "qty" : 22,
                                      |    "price" : 59.99,
                                      |    "ConformedId" : 737373,
                                      |    "ConformedQty" : "22",
                                      |    "ConformedPrice" : 59.99
                                      |  }, {
                                      |    "itemid" : "28781",
                                      |    "qty" : 10001,
                                      |    "price" : 99.95,
                                      |    "ConformedId" : 28781,
                                      |    "ConformedQty" : "10001",
                                      |    "ConformedPrice" : 99.95
                                      |  } ],
                                      |  "errCol" : [ ],
                                      |  "ConformedStr" : "3",
                                      |  "ConformedDate" : "2018-02-21",
                                      |  "ConformedTimestamp" : 1519200617
                                      |}, {
                                      |  "id" : 4,
                                      |  "date" : "2018-03-30",
                                      |  "placed" : "2018-03-30T00:22:10.121Z",
                                      |  "discount" : 0.850123456789,
                                      |  "items" : [ {
                                      |    "itemid" : "-123k",
                                      |    "qty" : 1,
                                      |    "price" : 33.66,
                                      |    "ConformedQty" : "1",
                                      |    "ConformedPrice" : 33.66
                                      |  } ],
                                      |  "errCol" : [ {
                                      |    "errType" : "confCastError",
                                      |    "errCode" : "E00003",
                                      |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
                                      |    "errCol" : "items.ConformedId",
                                      |    "rawValues" : [ "-123k" ],
                                      |    "mappings" : [ ]
                                      |  } ],
                                      |  "ConformedStr" : "4",
                                      |  "ConformedDate" : "2018-03-30",
                                      |  "ConformedTimestamp" : 1522369330,
                                      |  "ConformedDecimal" : 0.850123
                                      |} ]""".stripMargin.replace("\r\n", "\n")

}
